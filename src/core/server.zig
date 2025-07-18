const std = @import("std");
const Config = @import("../config.zig").Config;
const VirtualHost = @import("vhost.zig").VirtualHost;

pub const Server = struct {
    allocator: std.mem.Allocator,
    config: Config,
    vhosts: std.HashMap([]const u8, *VirtualHost, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    connections: std.ArrayList(*Connection),
    next_connection_id: u64,
    running: bool,
    tcp_server: ?std.net.Server,
    cli_server: ?std.net.Server,

    const Connection = struct {
        id: u64,
        socket: std.net.Stream,
        authenticated: bool,
        virtual_host: ?[]const u8,
        channels: std.HashMap(u16, *Channel, std.hash_map.AutoContext(u16), std.hash_map.default_max_load_percentage),
        heartbeat_interval: u16,
        last_heartbeat: i64,
        allocator: std.mem.Allocator,

        const Channel = struct {
            id: u16,
            active: bool,
            flow_active: bool,
            connection_id: u64,
            allocator: std.mem.Allocator,

            pub fn init(allocator: std.mem.Allocator, id: u16, connection_id: u64) Channel {
                return Channel{
                    .id = id,
                    .active = true,
                    .flow_active = true,
                    .connection_id = connection_id,
                    .allocator = allocator,
                };
            }

            pub fn deinit(self: *Channel) void {
                _ = self;
                // Cleanup channel resources
            }
        };

        pub fn init(allocator: std.mem.Allocator, id: u64, socket: std.net.Stream) Connection {
            return Connection{
                .id = id,
                .socket = socket,
                .authenticated = false,
                .virtual_host = null,
                .channels = std.HashMap(u16, *Channel, std.hash_map.AutoContext(u16), std.hash_map.default_max_load_percentage).init(allocator),
                .heartbeat_interval = 0,
                .last_heartbeat = std.time.timestamp(),
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Connection) void {
            var iterator = self.channels.iterator();
            while (iterator.next()) |entry| {
                entry.value_ptr.*.deinit();
                self.allocator.destroy(entry.value_ptr.*);
            }
            self.channels.deinit();
            self.socket.close();
        }

        pub fn addChannel(self: *Connection, channel_id: u16) !void {
            const channel = try self.allocator.create(Channel);
            channel.* = Channel.init(self.allocator, channel_id, self.id);
            try self.channels.put(channel_id, channel);
        }

        pub fn removeChannel(self: *Connection, channel_id: u16) void {
            if (self.channels.fetchRemove(channel_id)) |entry| {
                entry.value.deinit();
                self.allocator.destroy(entry.value);
            }
        }

        pub fn getChannel(self: *Connection, channel_id: u16) ?*Channel {
            return self.channels.get(channel_id);
        }
    };

    pub fn init(allocator: std.mem.Allocator, config: Config) !Server {
        var server = Server{
            .allocator = allocator,
            .config = config,
            .vhosts = std.HashMap([]const u8, *VirtualHost, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .connections = std.ArrayList(*Connection).init(allocator),
            .next_connection_id = 1,
            .running = false,
            .tcp_server = null,
            .cli_server = null,
        };

        // Create default virtual host
        try server.createVirtualHost("/");

        return server;
    }

    pub fn deinit(self: *Server) void {
        self.stop();

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
        const connection = try self.allocator.create(Connection);
        connection.* = Connection.init(self.allocator, self.next_connection_id, client_socket.stream);
        self.next_connection_id += 1;

        try self.connections.append(connection);

        std.log.info("New connection established: {}", .{connection.id});

        // TODO: Implement AMQP protocol handshake and message handling
        self.performHandshake(connection) catch |err| {
            std.log.err("Handshake failed for connection {}: {}", .{ connection.id, err });
            self.removeConnection(connection);
            return;
        };

        // TODO: Start message processing loop
        self.processMessages(connection) catch |err| {
            std.log.err("Message processing failed for connection {}: {}", .{ connection.id, err });
            self.removeConnection(connection);
        };
    }

    fn performHandshake(self: *Server, connection: *Connection) !void {
        _ = self;
        // TODO: Implement AMQP handshake
        // 1. Send Connection.Start
        // 2. Receive Connection.StartOk
        // 3. Send Connection.Tune
        // 4. Receive Connection.TuneOk
        // 5. Receive Connection.Open
        // 6. Send Connection.OpenOk
        std.log.info("AMQP handshake completed for connection {}", .{connection.id});
    }

    fn processMessages(self: *Server, connection: *Connection) !void {
        // TODO: Implement message processing loop
        // 1. Read frames from socket
        // 2. Parse AMQP methods
        // 3. Dispatch to appropriate handlers
        // 4. Send responses

        var buffer: [8192]u8 = undefined;
        while (self.running) {
            const bytes_read = connection.socket.read(&buffer) catch |err| {
                std.log.debug("Connection {} read error: {}", .{ connection.id, err });
                break;
            };

            if (bytes_read == 0) break;

            // TODO: Process received data
            std.log.debug("Received {} bytes from connection {}", .{ bytes_read, connection.id });
        }
    }

    fn removeConnection(self: *Server, connection: *Connection) void {
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
