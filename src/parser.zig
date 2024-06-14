const std = @import("std");

const Null = struct {};

const SimpleError = struct {
    const Self = @This();

    data: []const u8,

    pub fn init(data: []const u8) Self {
        return .{ .data = data };
    }
};

const SimpleString = struct {
    const Self = @This();

    data: []const u8,

    pub fn init(data: []const u8) Self {
        return .{ .data = data };
    }
};

const BulkString = struct {
    const Self = @This();

    length: usize,
    data: []const u8,

    pub fn init(length: usize, data: []const u8) Self {
        return .{ .length = length, .data = data };
    }
};

const Array = struct {
    const Self = @This();

    size: usize,
    data: []Data,

    pub fn init(size: usize, data: []Data) Self {
        return .{ .size = size, .data = data };
    }
};

const DeserializeError = error{
    OutOfMemory,
    InvalidType,
};

const Data = union(enum) {
    null: Null,
    simple_error: SimpleError,
    simple_string: SimpleString,
    bulk_string: BulkString,
    array: Array,
};

const Result = struct {
    data: Data,
    bytes_read: usize,
};

pub const Parser = struct {
    const Self = @This();

    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: Self) void {
        _ = self;
    }

    pub fn deserialize(self: Self, str: []const u8) DeserializeError!Result {
        return switch (str[0]) {
            '-' => self.simple_error(str),
            '+' => self.simple_string(str),
            '$' => self.bulk_string(str),
            '*' => self.array(str),
            else => return DeserializeError.InvalidType,
        };
    }

    fn parse_simple_string(self: Self, resp: []const u8) !struct {
        []const u8,
        usize,
    } {
        var i: usize = 1;
        var capacity: usize = 10;
        var str = try self.allocator.alloc(u8, capacity);

        while (resp[i] != '\r') {
            if (i == capacity) {
                capacity *= 2;
                str = try self.allocator.realloc(str, capacity);
            }

            str[i - 1] = resp[i];
            i += 1;
        }

        return .{ str[0 .. i - 1], i + 2 };
    }

    fn parse_bulk_string(self: Self, resp: []const u8) !struct {
        ?usize,
        ?[]const u8,
        usize,
    } {
        var i: usize = 1;
        var length: usize = 0;
        var is_negative = false;

        if (resp[i] == '-') {
            is_negative = true;
        }

        while (resp[i] != '\r') {
            length = length * 10 + resp[i] - '0';
            i += 1;
        }

        i += 2;

        if (is_negative) {
            return .{ null, null, i };
        }

        var string = try self.allocator.alloc(u8, length);
        var j: usize = 0;

        while (resp[i] != '\r') {
            string[j] = resp[i];
            i += 1;
            j += 1;
        }

        return .{ length, string, i + 2 };
    }

    fn simple_error(self: Self, resp: []const u8) !Result {
        const result = try self.parse_simple_string(resp);

        return .{
            .data = .{ .simple_error = SimpleError.init(result[0]) },
            .bytes_read = result[1],
        };
    }

    fn simple_string(self: Self, resp: []const u8) !Result {
        const result = try self.parse_simple_string(resp);

        return .{
            .data = .{ .simple_string = SimpleString.init(result[0]) },
            .bytes_read = result[1],
        };
    }

    fn bulk_string(self: Self, resp: []const u8) !Result {
        const result = try self.parse_bulk_string(resp);

        if (result[0] != null and result[1] != null) {
            return .{
                .data = .{ .bulk_string = BulkString.init(result[0].?, result[1].?) },
                .bytes_read = result[2],
            };
        }

        return .{
            .data = .{ .null = Null{} },
            .bytes_read = result[2],
        };
    }

    fn array(self: Self, resp: []const u8) !Result {
        var i: usize = 1;
        var length: usize = 0;

        while (resp[i] != '\r') {
            length = length * 10 + resp[i] - '0';
            i += 1;
        }

        i += 2;

        var data = try self.allocator.alloc(Data, length);

        for (0..length) |j| {
            const result = try self.deserialize(resp[i..]);
            i += result.bytes_read;
            data[j] = result.data;
        }

        return .{
            .data = .{ .array = Array.init(length, data) },
            .bytes_read = i,
        };
    }
};

test "deserialize" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const parser = Parser.init(allocator);
    defer parser.deinit();

    {
        const result = try parser.deserialize("+lorem ipsum dolor sit amet\r\n");
        try std.testing.expect(switch (result) {
            .simple_string => true,
            else => false,
        });
        try std.testing.expectEqualStrings("lorem ipsum dolor sit amets", result.simple_string.data);
    }

    {
        const result = try parser.deserialize("$3\r\nget\r\n");
        try std.testing.expect(switch (result) {
            .bulk_string => true,
            else => false,
        });
        try std.testing.expectEqual(3, result.bulk_string.length);
        try std.testing.expectEqualStrings("get", result.bulk_string.data);
    }

    {
        const result = try parser.deserialize("$-1\r\n");
        try std.testing.expect(switch (result) {
            .null => true,
            else => false,
        });
    }

    {
        const result = try parser.deserialize("-Error\r\n");
        try std.testing.expect(switch (result) {
            .simple_error => true,
            else => false,
        });
        try std.testing.expectEqualStrings("Error", result.simple_error.data);
    }

    {
        const result = try parser.deserialize("*1\r\n$3\r\nget\r\n");
        try std.testing.expect(switch (result) {
            .array => true,
            else => false,
        });
        // try std.testing.expectEqualStrings("Error", result.simple_error.data);
    }
}
