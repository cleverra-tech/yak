const std = @import("std");
const Config = @import("../config.zig").Config;
const VirtualHost = @import("vhost.zig").VirtualHost;
const Frame = @import("../protocol/frame.zig").Frame;
const FrameType = @import("../protocol/frame.zig").FrameType;
const methods = @import("../protocol/methods.zig");
const NetworkConnection = @import("../network/connection.zig").Connection;
const ConnectionState = @import("../network/connection.zig").ConnectionState;

pub const Server = struct {
    allocator: std.mem.Allocator,
    config: Config,
    vhosts: std.HashMap([]const u8, *VirtualHost, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    connections: std.ArrayList(*NetworkConnection),
    next_connection_id: u64,
    running: bool,
    tcp_server: ?std.net.Server,
    cli_server: ?std.net.Server,


    pub fn init(allocator: std.mem.Allocator, config: Config) !Server {
        var server = Server{
            .allocator = allocator,
            .config = config,
            .vhosts = std.HashMap([]const u8, *VirtualHost, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .connections = std.ArrayList(*NetworkConnection).init(allocator),
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
        const connection = try self.allocator.create(NetworkConnection);
        connection.* = try NetworkConnection.init(self.allocator, self.next_connection_id, client_socket.stream);
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

    fn performHandshake(self: *Server, connection: *NetworkConnection) !void {
        std.log.debug("Starting AMQP handshake for connection {}", .{connection.id});

        // Step 1: Send protocol header ("AMQP\x00\x00\x09\x01")
        const protocol_header = "AMQP\x00\x00\x09\x01";
        _ = try connection.socket.write(protocol_header);
        std.log.debug("Protocol header sent to connection {}", .{connection.id});

        // Step 2: Send Connection.Start
        try self.sendConnectionStart(connection);
        connection.setState(.start_sent);

        // Step 3: Receive Connection.StartOk
        const start_ok_frame = try self.receiveFrame(connection) orelse return error.ConnectionClosed;
        defer start_ok_frame.deinit(connection.allocator);
        try self.handleConnectionStartOk(connection, start_ok_frame);
        connection.setState(.start_ok_received);

        // Step 4: Send Connection.Tune
        try self.sendConnectionTune(connection);
        connection.setState(.tune_sent);

        // Step 5: Receive Connection.TuneOk
        const tune_ok_frame = try self.receiveFrame(connection) orelse return error.ConnectionClosed;
        defer tune_ok_frame.deinit(connection.allocator);
        try self.handleConnectionTuneOk(connection, tune_ok_frame);
        connection.setState(.tune_ok_received);

        // Step 6: Receive Connection.Open
        const open_frame = try self.receiveFrame(connection) orelse return error.ConnectionClosed;
        defer open_frame.deinit(connection.allocator);
        try self.handleConnectionOpen(connection, open_frame);
        connection.setState(.open_received);

        // Step 7: Send Connection.OpenOk
        try self.sendConnectionOpenOk(connection);
        connection.setState(.open_ok_sent);
        connection.setState(.open);
        connection.authenticated = true;

        std.log.info("AMQP handshake completed for connection {}", .{connection.id});
    }

    fn processMessages(self: *Server, connection: *NetworkConnection) !void {
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

    // AMQP Handshake Helper Methods
    fn sendConnectionStart(self: *Server, connection: *NetworkConnection) !void {
        _ = self;
        var buffer = std.ArrayList(u8).init(connection.allocator);
        defer buffer.deinit();

        // Create Connection.Start method arguments
        try buffer.writer().writeInt(u8, 0, .big); // version_major
        try buffer.writer().writeInt(u8, 9, .big); // version_minor

        // Server properties (empty field table for now)
        try buffer.writer().writeInt(u32, 0, .big); // field table length

        // Mechanisms (PLAIN only for now)
        const mechanisms = "PLAIN";
        try buffer.writer().writeInt(u32, @intCast(mechanisms.len), .big);
        try buffer.writer().writeAll(mechanisms);

        // Locales
        const locales = "en_US";
        try buffer.writer().writeInt(u32, @intCast(locales.len), .big);
        try buffer.writer().writeAll(locales);

        // Create method frame
        var method_buffer = std.ArrayList(u8).init(connection.allocator);
        defer method_buffer.deinit();

        try method_buffer.writer().writeInt(u16, @intFromEnum(methods.ClassId.connection), .big);
        try method_buffer.writer().writeInt(u16, @intFromEnum(methods.ConnectionMethod.start), .big);
        try method_buffer.appendSlice(buffer.items);

        const frame = Frame.createMethod(0, method_buffer.items);
        const encoded_frame = try frame.encode(connection.allocator);
        defer connection.allocator.free(encoded_frame);

        _ = try connection.socket.write(encoded_frame);
        std.log.debug("Connection.Start sent to connection {}", .{connection.id});
    }

    fn handleConnectionStartOk(self: *Server, connection: *NetworkConnection, frame: Frame) !void {
        _ = self;
        if (frame.frame_type != .method or frame.channel_id != 0) {
            return error.InvalidFrame;
        }

        if (frame.payload.len < 4) return error.InvalidMethodFrame;
        
        const class_id = std.mem.readInt(u16, frame.payload[0..2], .big);
        const method_id = std.mem.readInt(u16, frame.payload[2..4], .big);

        if (class_id != @intFromEnum(methods.ClassId.connection) or 
            method_id != @intFromEnum(methods.ConnectionMethod.start_ok)) {
            return error.UnexpectedMethod;
        }

        std.log.debug("Connection.StartOk received from connection {}", .{connection.id});
    }

    fn sendConnectionTune(self: *Server, connection: *NetworkConnection) !void {
        var buffer = std.ArrayList(u8).init(connection.allocator);
        defer buffer.deinit();

        // Create Connection.Tune method arguments
        try buffer.writer().writeInt(u16, self.config.limits.channel_max, .big); // channel_max
        try buffer.writer().writeInt(u32, self.config.limits.max_frame_size, .big); // frame_max
        try buffer.writer().writeInt(u16, self.config.limits.heartbeat_interval, .big); // heartbeat

        // Create method frame
        var method_buffer = std.ArrayList(u8).init(connection.allocator);
        defer method_buffer.deinit();

        try method_buffer.writer().writeInt(u16, @intFromEnum(methods.ClassId.connection), .big);
        try method_buffer.writer().writeInt(u16, @intFromEnum(methods.ConnectionMethod.tune), .big);
        try method_buffer.appendSlice(buffer.items);

        const frame = Frame.createMethod(0, method_buffer.items);
        const encoded_frame = try frame.encode(connection.allocator);
        defer connection.allocator.free(encoded_frame);

        _ = try connection.socket.write(encoded_frame);
        std.log.debug("Connection.Tune sent to connection {}", .{connection.id});
    }

    fn handleConnectionTuneOk(self: *Server, connection: *NetworkConnection, frame: Frame) !void {
        if (frame.frame_type != .method or frame.channel_id != 0) {
            return error.InvalidFrame;
        }

        if (frame.payload.len < 12) return error.InvalidMethodFrame; // class + method + 3 parameters
        
        const class_id = std.mem.readInt(u16, frame.payload[0..2], .big);
        const method_id = std.mem.readInt(u16, frame.payload[2..4], .big);

        if (class_id != @intFromEnum(methods.ClassId.connection) or 
            method_id != @intFromEnum(methods.ConnectionMethod.tune_ok)) {
            return error.UnexpectedMethod;
        }

        // Parse negotiated parameters
        const channel_max = std.mem.readInt(u16, frame.payload[4..6], .big);
        const frame_max = std.mem.readInt(u32, frame.payload[6..10], .big);
        const heartbeat = std.mem.readInt(u16, frame.payload[10..12], .big);

        // Update connection parameters (note: we should validate these against our limits)
        // For now, accept client's negotiated values but ensure they don't exceed our limits
        connection.channel_max = @min(channel_max, self.config.limits.channel_max);
        connection.max_frame_size = @min(frame_max, self.config.limits.max_frame_size);
        connection.heartbeat_interval = @min(heartbeat, self.config.limits.heartbeat_interval);

        std.log.debug("Connection.TuneOk received from connection {} (channel_max={}, frame_max={}, heartbeat={})", 
                     .{connection.id, channel_max, frame_max, heartbeat});
    }

    fn handleConnectionOpen(self: *Server, connection: *NetworkConnection, frame: Frame) !void {
        if (frame.frame_type != .method or frame.channel_id != 0) {
            return error.InvalidFrame;
        }

        if (frame.payload.len < 4) return error.InvalidMethodFrame;
        
        const class_id = std.mem.readInt(u16, frame.payload[0..2], .big);
        const method_id = std.mem.readInt(u16, frame.payload[2..4], .big);

        if (class_id != @intFromEnum(methods.ClassId.connection) or 
            method_id != @intFromEnum(methods.ConnectionMethod.open)) {
            return error.UnexpectedMethod;
        }

        // Parse virtual host name
        if (frame.payload.len < 5) return error.InvalidMethodFrame;
        const vhost_len = frame.payload[4];
        if (frame.payload.len < 5 + vhost_len) return error.InvalidMethodFrame;
        
        const vhost_name = frame.payload[5..5 + vhost_len];
        
        // Validate virtual host exists
        if (self.getVirtualHost(vhost_name) == null) {
            return error.VirtualHostNotFound;
        }

        // Set virtual host on connection
        try connection.setVirtualHost(vhost_name);

        std.log.debug("Connection.Open received from connection {} (vhost={s})", 
                     .{connection.id, vhost_name});
    }

    fn sendConnectionOpenOk(self: *Server, connection: *NetworkConnection) !void {
        _ = self;
        var buffer = std.ArrayList(u8).init(connection.allocator);
        defer buffer.deinit();

        // Connection.OpenOk has no arguments, just empty reserved field
        try buffer.writer().writeInt(u8, 0, .big); // reserved field (empty string)

        // Create method frame
        var method_buffer = std.ArrayList(u8).init(connection.allocator);
        defer method_buffer.deinit();

        try method_buffer.writer().writeInt(u16, @intFromEnum(methods.ClassId.connection), .big);
        try method_buffer.writer().writeInt(u16, @intFromEnum(methods.ConnectionMethod.open_ok), .big);
        try method_buffer.appendSlice(buffer.items);

        const frame = Frame.createMethod(0, method_buffer.items);
        const encoded_frame = try frame.encode(connection.allocator);
        defer connection.allocator.free(encoded_frame);

        _ = try connection.socket.write(encoded_frame);
        std.log.debug("Connection.OpenOk sent to connection {}", .{connection.id});
    }

    fn receiveFrame(self: *Server, connection: *NetworkConnection) !?Frame {
        _ = self;
        // Read frame header first (8 bytes: type + channel + size + end)
        var header_buf: [8]u8 = undefined;
        const header_bytes = connection.socket.read(&header_buf) catch |err| {
            std.log.debug("Connection {} read error: {}", .{ connection.id, err });
            return null;
        };

        if (header_bytes == 0) {
            return null; // Connection closed
        }

        if (header_bytes < 8) {
            return error.IncompleteFrame;
        }

        // Parse frame header
        const frame_type = @as(FrameType, @enumFromInt(header_buf[0]));
        const channel = (@as(u16, header_buf[1]) << 8) | header_buf[2];
        const payload_size = (@as(u32, header_buf[3]) << 24) |
            (@as(u32, header_buf[4]) << 16) |
            (@as(u32, header_buf[5]) << 8) |
            header_buf[6];
        const frame_end = header_buf[7];

        if (frame_end != 0xCE) {
            return error.InvalidFrameEnd;
        }

        if (payload_size > connection.max_frame_size) {
            return error.FrameTooLarge;
        }

        // Read payload
        const payload = try connection.allocator.alloc(u8, payload_size);
        
        const payload_bytes = try connection.socket.readAll(payload);
        if (payload_bytes != payload_size) {
            connection.allocator.free(payload);
            return error.IncompletePayload;
        }

        // Create and return frame
        const frame = Frame{
            .frame_type = frame_type,
            .channel_id = channel,
            .payload = payload,
        };

        std.log.debug("Frame received on connection {}: type={}, channel={}, size={}", 
                     .{ connection.id, frame_type, channel, payload_size });

        return frame;
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
