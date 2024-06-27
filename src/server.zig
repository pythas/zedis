const std = @import("std");
const net = std.net;
const Parser = @import("parser.zig").Parser;
const Data = @import("parser.zig").Data;
const SimpleString = @import("parser.zig").SimpleString;
const Null = @import("parser.zig").Null;

const Command = enum {
    ping,
    echo,
    set,
    get,
};

const Entry = struct {
    key: []const u8,
    value: []const u8,
};

pub const Server = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,
    store: std.hash_map.StringHashMap(Entry),
    parser: Parser,

    pub fn init(allocator: std.mem.Allocator) !Self {
        return .{
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
            .store = std.hash_map.StringHashMap(Entry).init(allocator),
            .parser = Parser.init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {

        // TODO: fixme
        // var it = self.store.iterator();
        // while (it.next()) |entry| {
        //     self.allocator.free(entry.key_ptr.*);
        //     self.allocator.free(entry.value_ptr.*);
        // }

        self.store.deinit();
        self.parser.deinit();
    }

    pub fn handleConnection(self: *Self, connection: net.Server.Connection) !void {
        defer connection.stream.close();

        const buffer = try self.allocator.alloc(u8, 512);
        defer self.allocator.free(buffer);

        std.debug.print("Connection received from {}\n", .{connection.address});

        while (true) {
            const read_size = try connection.stream.read(buffer);

            if (read_size == 0) {
                std.debug.print("Connection ended\n", .{});
                break;
            }

            const resp = buffer[0..read_size];

            const result = self.parser.deserialize(resp) catch {
                std.debug.print("Deserialization error\n", .{});
                break;
            };
            defer result.data.deinit(self.allocator);

            var cmd_buffer: [16]u8 = undefined;
            const cmd_str = std.ascii.lowerString(&cmd_buffer, result.data.array.data[0].bulk_string.data);
            const cmd = std.meta.stringToEnum(Command, cmd_str) orelse break;

            switch (cmd) {
                .ping => try self.handlePing(connection),
                .echo => try self.handleEcho(connection, result.data.array.data[1].bulk_string.data),
                .set => try self.handleSet(connection, result.data.array.data[1].bulk_string.data, result.data.array.data[2].bulk_string.data),
                .get => try self.handleGet(connection, result.data.array.data[1].bulk_string.data),
            }
        }
    }

    fn handlePing(self: *Self, connection: net.Server.Connection) !void {
        const data = Data{ .simple_string = SimpleString.init("PONG") };
        const string = try self.parser.serialize(data);
        defer self.allocator.free(string);
        _ = try connection.stream.write(string);
    }

    fn handleEcho(self: *Self, connection: net.Server.Connection, message: []const u8) !void {
        const data = Data{ .simple_string = SimpleString.init(message) };
        const string = try self.parser.serialize(data);
        defer self.allocator.free(string);
        _ = try connection.stream.write(string);
    }

    fn handleSet(self: *Self, connection: net.Server.Connection, key: []const u8, value: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const key_copy = try self.allocator.dupe(u8, key);
        const value_copy = try self.allocator.dupe(u8, value);

        const entry = Entry{
            .key = key_copy,
            .value = value_copy,
        };

        if (self.store.get(key)) |old_entry| {
            _ = self.store.remove(key);
            self.allocator.free(old_entry.key);
            self.allocator.free(old_entry.value);
        }

        try self.store.put(key_copy, entry);

        const data = Data{ .simple_string = SimpleString.init("OK") };
        const string = try self.parser.serialize(data);
        defer self.allocator.free(string);
        _ = try connection.stream.write(string);
    }

    fn handleGet(self: *Self, connection: net.Server.Connection, key: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const maybe_entry = self.store.get(key);
        var data: Data = undefined;

        if (maybe_entry) |entry| {
            data = Data{ .simple_string = SimpleString.init(entry.value) };
        } else {
            data = Data{ .null = Null.init() };
        }

        const string = try self.parser.serialize(data);
        defer self.allocator.free(string);
        _ = try connection.stream.write(string);
    }
};
