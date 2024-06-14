const std = @import("std");
const Parser = @import("parser.zig").Parser;

pub fn main() !void {
    std.debug.print("\n", .{});

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const parser = Parser.init(allocator);
    defer parser.deinit();

    const result = try parser.deserialize("*2\r\n*1\r\n$1\r\na\r\n$3\r\nget\r\n");
    std.debug.print("{!}\n", .{result.data.array});
}
