const std = @import("std");
const net = std.net;
const Parser = @import("parser.zig").Parser;
const Result = @import("parser.zig").Result;
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

    fn getArg(result: *const Result, index: usize) ![]const u8 {
        if (index >= result.data.array.data.len) {
            return error.IndexOutOfBounds;
        }

        const element = result.data.array.data[index];

        switch (element) {
            .bulk_string => |bulk_string| {
                if (bulk_string.data.len == 0) {
                    return error.InvalidArgument;
                }
                return bulk_string.data;
            },
            else => return error.InvalidArgument,
        }
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
            const cmd_str = std.ascii.lowerString(&cmd_buffer, try Self.getArg(&result, 0));
            const cmd = std.meta.stringToEnum(Command, cmd_str) orelse break;

            // TODO: catch and return errors to client
            switch (cmd) {
                .ping => try self.handlePing(connection),
                .echo => {
                    const arg = try Self.getArg(&result, 1);
                    try self.handleEcho(connection, arg);
                },
                .set => {
                    const arg1 = try Self.getArg(&result, 1);
                    const arg2 = try Self.getArg(&result, 2);
                    try self.handleSet(connection, arg1, arg2);
                },
                .get => {
                    const arg = try Self.getArg(&result, 1);
                    try self.handleGet(connection, arg);
                },
            }
        }
    }

    fn handlePing(self: *Self, connection: net.Server.Connection) !void {
        const string = try self.parser.serialize(Data{
            .simple_string = SimpleString.init("PONG"),
        });
        defer self.allocator.free(string);

        _ = try connection.stream.write(string);
    }

    fn handleEcho(self: *Self, connection: net.Server.Connection, message: []const u8) !void {
        const string = try self.parser.serialize(Data{
            .simple_string = SimpleString.init(message),
        });
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

        const string = try self.parser.serialize(Data{
            .simple_string = SimpleString.init("OK"),
        });
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
