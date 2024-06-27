const std = @import("std");

pub const Null = struct {
    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    pub fn toString(_: Self) []const u8 {
        return "$-1\r\n";
    }

    pub fn deinit(_: Self, _: std.mem.Allocator) void {}
};

pub const SimpleError = struct {
    const Self = @This();

    data: []const u8,

    pub fn init(data: []const u8) Self {
        return .{ .data = data };
    }

    pub fn toString(self: Self, allocator: std.mem.Allocator) ![]const u8 {
        return std.fmt.allocPrint(allocator, "-{s}\r\n", .{self.data});
    }

    pub fn deinit(self: Self, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

pub const SimpleString = struct {
    const Self = @This();

    data: []const u8,

    pub fn init(data: []const u8) Self {
        return .{ .data = data };
    }

    pub fn toString(self: Self, allocator: std.mem.Allocator) ![]const u8 {
        return std.fmt.allocPrint(allocator, "+{s}\r\n", .{self.data});
    }

    pub fn deinit(self: Self, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

pub const BulkString = struct {
    const Self = @This();

    length: usize,
    data: []const u8,

    pub fn init(length: usize, data: []const u8) Self {
        return .{ .length = length, .data = data };
    }

    pub fn toString(self: Self, allocator: std.mem.Allocator) ![]const u8 {
        return std.fmt.allocPrint(allocator, "${d}\r\n{s}\r\n", .{
            self.length,
            self.data,
        });
    }

    pub fn deinit(self: Self, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

pub const Array = struct {
    const Self = @This();

    size: usize,
    data: []Data,

    pub fn init(size: usize, data: []Data) Self {
        return .{ .size = size, .data = data };
    }

    pub fn toString(self: Self, parser: Parser, allocator: std.mem.Allocator) ![]const u8 {
        var result = try std.fmt.allocPrint(allocator, "*{d}\r\n", .{self.size});

        for (self.data) |item| {
            const string = try parser.serialize(item);
            result = try std.fmt.allocPrint(allocator, "{s}{s}", .{ result, string });
        }

        return result;
    }

    pub fn deinit(self: Self, allocator: std.mem.Allocator) void {
        for (self.data) |item| {
            item.deinit(allocator);
        }
        allocator.free(self.data);
    }
};

const DeserializeError = error{
    OutOfMemory,
    InvalidType,
};

const SerializeError = error{
    OutOfMemory,
};

pub const Data = union(enum) {
    const Self = @This();

    null: Null,
    simple_error: SimpleError,
    simple_string: SimpleString,
    bulk_string: BulkString,
    array: Array,

    pub fn deinit(self: Self, allocator: std.mem.Allocator) void {
        switch (self) {
            .null => |*x| x.deinit(allocator),
            .simple_error => |*x| x.deinit(allocator),
            .simple_string => |*x| x.deinit(allocator),
            .bulk_string => |*x| x.deinit(allocator),
            .array => |*x| x.deinit(allocator),
        }
    }
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

    pub fn serialize(self: Self, data: Data) SerializeError![]const u8 {
        return switch (data) {
            .null => data.null.toString(),
            .simple_error => data.simple_error.toString(self.allocator),
            .simple_string => data.simple_string.toString(self.allocator),
            .bulk_string => data.bulk_string.toString(self.allocator),
            .array => data.array.toString(self, self.allocator),
        };
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

        while (resp[i] != '\r') {
            i += 1;
        }

        const string = resp[1..i];
        const string_copy = try self.allocator.dupe(u8, string);
        return .{ string_copy, i + 2 };
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
            if (!is_negative) {
                length = length * 10 + resp[i] - '0';
            }
            i += 1;
        }

        i += 2;

        if (is_negative) {
            return .{ null, null, i };
        }

        const string = resp[i .. i + length];
        const string_copy = try self.allocator.dupe(u8, string);

        return .{ length, string_copy, i + length + 2 };
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

test "serialize" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const parser = Parser.init(allocator);
    defer parser.deinit();

    {
        const data = Data{ .null = Null.init() };

        const result = try parser.serialize(data);
        try std.testing.expectEqualStrings("$-1\r\n", result);
    }

    {
        const data = Data{ .simple_error = SimpleError.init("Error") };

        const result = try parser.serialize(data);
        try std.testing.expectEqualStrings("-Error\r\n", result);
    }

    {
        const data = Data{ .simple_string = SimpleString.init("Message") };

        const result = try parser.serialize(data);
        try std.testing.expectEqualStrings("+Message\r\n", result);
    }

    {
        const data = Data{ .bulk_string = BulkString.init(4, "Bulk") };

        const result = try parser.serialize(data);
        try std.testing.expectEqualStrings("$4\r\nBulk\r\n", result);
    }

    {
        var array_data: [1]Data = undefined;
        array_data[0] = Data{ .simple_string = SimpleString.init("item") };
        const data = Data{ .array = Array.init(1, &array_data) };

        const result = try parser.serialize(data);
        try std.testing.expectEqualStrings("*1\r\n+item\r\n", result);
    }
}

test "deserialize" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const parser = Parser.init(allocator);
    defer parser.deinit();

    {
        const result = try parser.deserialize("+lorem ipsum dolor sit amet\r\n");
        try std.testing.expect(switch (result.data) {
            .simple_string => true,
            else => false,
        });
        try std.testing.expectEqualStrings("lorem ipsum dolor sit amet", result.data.simple_string.data);
    }

    {
        const result = try parser.deserialize("$3\r\nget\r\n");
        try std.testing.expect(switch (result.data) {
            .bulk_string => true,
            else => false,
        });
        try std.testing.expectEqual(3, result.data.bulk_string.length);
        try std.testing.expectEqualStrings("get", result.data.bulk_string.data);
    }

    {
        const result = try parser.deserialize("$-1\r\n");
        try std.testing.expect(switch (result.data) {
            .null => true,
            else => false,
        });
    }

    {
        const result = try parser.deserialize("-Error\r\n");
        try std.testing.expect(switch (result.data) {
            .simple_error => true,
            else => false,
        });
        try std.testing.expectEqualStrings("Error", result.data.simple_error.data);
    }

    {
        const result = try parser.deserialize("*1\r\n$3\r\nget\r\n");
        try std.testing.expect(switch (result.data) {
            .array => true,
            else => false,
        });
        try std.testing.expectEqual(1, result.data.array.size);
        try std.testing.expectEqual(3, result.data.array.data[0].bulk_string.length);
        try std.testing.expectEqualStrings("get", result.data.array.data[0].bulk_string.data);
    }
}
