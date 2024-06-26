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

const Zedis = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,
    store: std.hash_map.StringHashMap([]const u8),
    parser: Parser,

    pub fn init(allocator: std.mem.Allocator) !Self {
        return .{
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
            .store = std.hash_map.StringHashMap([]const u8).init(allocator),
            .parser = Parser.init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
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

            std.debug.print("Received data: {s}\n", .{buffer});

            const result = self.parser.deserialize(buffer) catch {
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
        _ = try connection.stream.write(try self.parser.serialize(data));
    }

    fn handleEcho(self: *Self, connection: net.Server.Connection, message: []const u8) !void {
        const data = Data{ .simple_string = SimpleString.init(message) };
        _ = try connection.stream.write(try self.parser.serialize(data));
    }

    fn handleSet(self: *Self, connection: net.Server.Connection, key: []const u8, value: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.store.put(key, value);
        const data = Data{ .simple_string = SimpleString.init("OK") };
        _ = try connection.stream.write(try self.parser.serialize(data));
    }

    fn handleGet(self: *Self, connection: net.Server.Connection, key: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const maybe_value = self.store.get(key);
        var data: Data = undefined;

        if (maybe_value) |value| {
            data = Data{ .simple_string = SimpleString.init(value) };
        } else {
            data = Data{ .null = Null.init() };
        }

        _ = try connection.stream.write(try self.parser.serialize(data));
    }
};

pub fn main() !void {
    std.debug.print("\n", .{});

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var zedis = try Zedis.init(allocator);
    defer zedis.deinit();

    const localhost = net.Address{ .in = try net.Ip4Address.parse("127.0.0.1", 6379) };
    var server = try localhost.listen(.{ .reuse_port = true });
    defer server.deinit();

    std.debug.print("Listening on {}\n", .{server.listen_address});

    while (true) {
        const connection = try server.accept();

        const thread = try std.Thread.spawn(.{}, handleClient, .{ &zedis, connection });
        defer thread.detach();
    }
}

fn handleClient(zedis: *Zedis, connection: net.Server.Connection) void {
    zedis.handleConnection(connection) catch |err| {
        std.debug.print("Connection handling error: {}\n", .{err});
    };
}

test {
    std.testing.refAllDecls(@This());
}
