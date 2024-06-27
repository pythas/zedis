const std = @import("std");
const net = std.net;
const Server = @import("server.zig").Server;

pub fn main() !void {
    std.debug.print("\n", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    // TODO: figure out why this allocator crashes
    // var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    // defer arena.deinit();
    // const allocator = arena.allocator();

    var zedis_server = try Server.init(allocator);
    defer zedis_server.deinit();

    const localhost = net.Address{ .in = try net.Ip4Address.parse("127.0.0.1", 6379) };
    var server = try localhost.listen(.{ .reuse_port = true });
    defer server.deinit();

    std.debug.print("Listening on {}\n", .{server.listen_address});

    while (true) {
        const connection = try server.accept();

        const thread = try std.Thread.spawn(.{}, handleClient, .{ &zedis_server, connection });
        defer thread.detach();
    }
}

fn handleClient(server: *Server, connection: net.Server.Connection) void {
    server.handleConnection(connection) catch |err| {
        std.debug.print("Connection handling error: {}\n", .{err});
    };
}

test {
    std.testing.refAllDecls(@This());
}
