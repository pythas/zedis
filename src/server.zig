const std = @import("std");
const assert = std.debug.assert;
const net = std.net;
const Parser = @import("parser.zig").Parser;
const Result = @import("parser.zig").Result;
const Data = @import("parser.zig").Data;
const SimpleString = @import("parser.zig").SimpleString;
const SimpleError = @import("parser.zig").SimpleError;
const Null = @import("parser.zig").Null;
const SerializeError = @import("parser.zig").SerializeError;
const DeserializeError = @import("parser.zig").DeserializeError;

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

const ServerError = error{
    SerializeError,
    DeserializeError,
    InvalidNumberOfArguments,
    InvalidArgument,
    IndexOutOfBounds,
    OutOfMemory,
    InvalidCommand,
};

const StreamError = std.posix.WriteError;

const Error = ServerError || StreamError;

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

    fn getNumArgs(result: Result) usize {
        return result.data.array.data.len;
    }

    fn assertNumArgs(result: Result, num: u8) Error!void {
        if (Self.getNumArgs(result) != num) {
            return error.InvalidNumberOfArguments;
        }
    }

    fn getArg(result: Result, index: usize) ![]const u8 {
        assert(index < result.data.array.data.len);

        const element = result.data.array.data[index];

        switch (element) {
            .bulk_string => |bulk_string| return bulk_string.data,
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

            const arg = try Self.getArg(result, 0);

            if (arg.len > 16) {
                try self.writeError(connection, "ERR command too long");
                continue;
            }

            var cmd_buffer: [16]u8 = undefined;
            const cmd_str = std.ascii.lowerString(&cmd_buffer, arg);
            const cmd = std.meta.stringToEnum(Command, cmd_str) orelse {
                try self.writeError(connection, "ERR unknown command");
                continue;
            };

            self.handleCmd(connection, cmd, result) catch |err| {
                switch (err) {
                    error.InvalidNumberOfArguments => try self.writeError(connection, "ERR wrong number of arguments for command"),
                    error.SerializeError => try self.writeError(connection, "ERR serialization error"),
                    error.DeserializeError => try self.writeError(connection, "ERR deserialization error"),
                    else => try self.writeError(connection, "ERR unknown error"),
                }
            };
        }
    }

    fn handleCmd(self: *Self, connection: net.Server.Connection, cmd: Command, result: Result) Error!void {
        switch (cmd) {
            .ping => try self.handlePing(connection),
            .echo => {
                try Self.assertNumArgs(result, 2);

                const arg = try Self.getArg(result, 1);

                try self.handleEcho(connection, arg);
            },
            .set => {
                try Self.assertNumArgs(result, 3);

                const arg1 = try Self.getArg(result, 1);
                const arg2 = try Self.getArg(result, 2);

                try self.handleSet(connection, arg1, arg2);
            },
            .get => {
                try Self.assertNumArgs(result, 2);

                const arg = try Self.getArg(result, 1);

                try self.handleGet(connection, arg);
            },
        }
    }

    fn handlePing(self: *Self, connection: net.Server.Connection) Error!void {
        try self.writeString(connection, "PONG");
    }

    fn handleEcho(self: *Self, connection: net.Server.Connection, message: []const u8) !void {
        try self.writeString(connection, message);
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

        try self.writeString(connection, "OK");
    }

    fn handleGet(self: *Self, connection: net.Server.Connection, key: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const maybe_entry = self.store.get(key);

        if (maybe_entry) |entry| {
            try self.writeString(connection, entry.value);
        } else {
            try self.writeNull(connection);
        }
    }

    fn write(connection: net.Server.Connection, data: []const u8) void {
        _ = connection.stream.write(data) catch |err| {
            std.debug.print("Error writing to stream: {}\n", .{err});
        };
    }

    fn writeError(self: Self, connection: net.Server.Connection, message: []const u8) !void {
        const string = try self.parser.serialize(Data{
            .simple_error = SimpleError.init(message),
        });
        defer self.allocator.free(string);

        Self.write(connection, string);
    }

    fn writeString(self: Self, connection: net.Server.Connection, message: []const u8) !void {
        const string = try self.parser.serialize(Data{
            .simple_string = SimpleString.init(message),
        });
        defer self.allocator.free(string);

        Self.write(connection, string);
    }

    fn writeNull(self: Self, connection: net.Server.Connection) !void {
        Self.write(connection, try self.parser.serialize(Data{ .null = Null.init() }));
    }
};
