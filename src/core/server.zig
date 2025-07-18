const std = @import("std");
const Config = @import("../config.zig").Config;
const VirtualHost = @import("vhost.zig").VirtualHost;
const NetworkConnection = @import("../network/connection.zig").Connection;
const ProtocolHandler = @import("../network/protocol.zig").ProtocolHandler;

pub const Server = struct {
    allocator: std.mem.Allocator,
    config: Config,
    vhosts: std.HashMap([]const u8, *VirtualHost, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    connections: std.ArrayList(*NetworkConnection),
    next_connection_id: u64,
    running: bool,
    tcp_server: ?std.net.Server,
    cli_server: ?std.net.Server,
    protocol_handler: ProtocolHandler,

    pub fn init(allocator: std.mem.Allocator, config: Config) !Server {
        const protocol_handler = ProtocolHandler.init(allocator);

        var server = Server{
            .allocator = allocator,
            .config = config,
            .vhosts = std.HashMap([]const u8, *VirtualHost, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .connections = std.ArrayList(*NetworkConnection).init(allocator),
            .next_connection_id = 1,
            .running = false,
            .tcp_server = null,
            .cli_server = null,
            .protocol_handler = protocol_handler,
        };

        // Create default virtual host
        try server.createVirtualHost("/");

        // Note: Virtual host integration will be handled within the protocol handler
        // using the connection's setVirtualHost method

        return server;
    }

    pub fn deinit(self: *Server) void {
        self.stop();

        // Clean up protocol handler
        self.protocol_handler.deinit();

        // Clean up connections
        for (self.connections.items) |connection| {
            connection.deinit();
            self.allocator.destroy(connection);
        }
        self.connections.deinit();

        // Clean up virtual hosts
        var vhost_iterator = self.vhosts.iterator();
        while (vhost_iterator.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.vhosts.deinit();
    }

    pub fn start(self: *Server) !void {
        std.log.info("Starting Yak AMQP server on {s}:{}", .{ self.config.tcp.host, self.config.tcp.port });

        const address = try std.net.Address.parseIp(self.config.tcp.host, self.config.tcp.port);
        self.tcp_server = try address.listen(.{
            .reuse_address = true,
        });

        self.running = true;

        // Start accepting connections
        while (self.running) {
            const client_socket = self.tcp_server.?.accept() catch |err| {
                std.log.err("Failed to accept connection: {}", .{err});
                continue;
            };

            // Handle connection in a separate thread (simplified for now)
            self.handleConnection(client_socket) catch |err| {
                std.log.err("Failed to handle connection: {}", .{err});
                client_socket.stream.close();
            };
        }
    }

    pub fn startWithShutdownMonitoring(self: *Server, shutdown_flag: *const bool) !void {
        std.log.info("Starting Yak AMQP server on {s}:{}", .{ self.config.tcp.host, self.config.tcp.port });

        const address = try std.net.Address.parseIp(self.config.tcp.host, self.config.tcp.port);
        self.tcp_server = try address.listen(.{
            .reuse_address = true,
        });

        self.running = true;

        // Start accepting connections with shutdown monitoring
        while (self.running and !shutdown_flag.*) {
            // Use accept with timeout to periodically check shutdown flag
            const client_socket = self.acceptWithTimeout(100) catch |err| switch (err) {
                error.WouldBlock => continue, // Timeout, check shutdown flag
                else => {
                    std.log.err("Failed to accept connection: {}", .{err});
                    continue;
                },
            };

            if (client_socket) |socket| {
                // Handle connection in a separate thread (simplified for now)
                self.handleConnection(socket) catch |err| {
                    std.log.err("Failed to handle connection: {}", .{err});
                    socket.stream.close();
                };
            }
        }

        if (shutdown_flag.*) {
            std.log.info("Shutdown signal received, stopping server gracefully", .{});
            self.stop();
        }
    }

    fn acceptWithTimeout(self: *Server, timeout_ms: u32) !?std.net.Server.Connection {
        _ = timeout_ms;

        // Use poll to check if socket is ready with timeout
        var pollfds = [_]std.posix.pollfd{
            std.posix.pollfd{
                .fd = self.tcp_server.?.stream.handle,
                .events = std.posix.POLL.IN,
                .revents = 0,
            },
        };

        const poll_result = try std.posix.poll(&pollfds, 100); // 100ms timeout
        if (poll_result == 0) {
            return null; // Timeout
        }

        return try self.tcp_server.?.accept();
    }

    pub fn stop(self: *Server) void {
        self.running = false;
        if (self.tcp_server) |*server| {
            server.deinit();
            self.tcp_server = null;
        }
        if (self.cli_server) |*server| {
            server.deinit();
            self.cli_server = null;
        }
    }

    pub fn startCliServer(self: *Server) !void {
        if (!self.config.cli.enabled) return;

        std.log.info("Starting CLI server on {s}", .{self.config.cli.socket_path});

        // Remove existing socket file if it exists
        std.fs.cwd().deleteFile(self.config.cli.socket_path) catch {};

        const address = try std.net.Address.initUnix(self.config.cli.socket_path);
        self.cli_server = try address.listen(.{});

        // TODO: Start CLI server in separate thread
    }

    fn handleConnection(self: *Server, client_socket: std.net.Server.Connection) !void {
        const connection = try self.allocator.create(NetworkConnection);
        connection.* = try NetworkConnection.init(self.allocator, self.next_connection_id, client_socket.stream);
        self.next_connection_id += 1;

        try self.connections.append(connection);

        std.log.info("New connection established: {}", .{connection.id});

        // Use protocol handler for connection management
        self.protocol_handler.handleConnection(connection) catch |err| {
            std.log.err("Protocol handling failed for connection {}: {}", .{ connection.id, err });
            self.removeConnection(connection);
        };
    }

    fn removeConnection(self: *Server, connection: *NetworkConnection) void {
        for (self.connections.items, 0..) |conn, i| {
            if (conn.id == connection.id) {
                _ = self.connections.swapRemove(i);
                conn.deinit();
                self.allocator.destroy(conn);
                break;
            }
        }
        std.log.info("Connection {} removed", .{connection.id});
    }

    pub fn createVirtualHost(self: *Server, name: []const u8) !void {
        const owned_name = try self.allocator.dupe(u8, name);
        const vhost = try self.allocator.create(VirtualHost);
        vhost.* = try VirtualHost.init(self.allocator, owned_name);

        try self.vhosts.put(owned_name, vhost);
        std.log.info("Virtual host created: {s}", .{name});
    }

    pub fn getVirtualHost(self: *Server, name: []const u8) ?*VirtualHost {
        return self.vhosts.get(name);
    }

    pub fn getConnectionCount(self: *const Server) u32 {
        return @intCast(self.connections.items.len);
    }

    pub fn getVirtualHostCount(self: *const Server) u32 {
        return @intCast(self.vhosts.count());
    }

    // Metrics and statistics
    pub fn getStats(self: *const Server, allocator: std.mem.Allocator) !std.json.Value {
        var stats = std.json.ObjectMap.init(allocator);

        try stats.put("connections", std.json.Value{ .integer = @intCast(self.connections.items.len) });
        try stats.put("virtual_hosts", std.json.Value{ .integer = @intCast(self.vhosts.count()) });
        try stats.put("running", std.json.Value{ .bool = self.running });

        return std.json.Value{ .object = stats };
    }
};

test "server creation and basic operations" {
    const allocator = std.testing.allocator;

    var config = try Config.default(allocator);
    defer config.deinit(allocator);

    var server = try Server.init(allocator, config);
    defer server.deinit();

    try std.testing.expectEqual(@as(u32, 1), server.getVirtualHostCount());
    try std.testing.expectEqual(@as(u32, 0), server.getConnectionCount());

    try server.createVirtualHost("test");
    try std.testing.expectEqual(@as(u32, 2), server.getVirtualHostCount());

    const vhost = server.getVirtualHost("/");
    try std.testing.expect(vhost != null);
}
