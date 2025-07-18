const std = @import("std");
const Connection = @import("connection.zig").Connection;
const ConnectionState = @import("connection.zig").ConnectionState;
const Frame = @import("../protocol/frame.zig").Frame;
const FrameType = @import("../protocol/frame.zig").FrameType;
const Method = @import("../protocol/methods.zig").Method;
const MethodClass = @import("../protocol/methods.zig").MethodClass;
const VirtualHost = @import("../core/vhost.zig").VirtualHost;

pub const ProtocolHandler = struct {
    allocator: std.mem.Allocator,
    server_properties: std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),

    pub fn init(allocator: std.mem.Allocator) ProtocolHandler {
        var handler = ProtocolHandler{
            .allocator = allocator,
            .server_properties = std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
        };

        // Initialize server properties
        handler.initServerProperties() catch |err| {
            std.log.err("Failed to initialize server properties: {}", .{err});
        };

        return handler;
    }

    pub fn deinit(self: *ProtocolHandler) void {
        var iterator = self.server_properties.iterator();
        while (iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.server_properties.deinit();
    }

    fn initServerProperties(self: *ProtocolHandler) !void {
        try self.server_properties.put(try self.allocator.dupe(u8, "product"), try self.allocator.dupe(u8, "Yak AMQP Message Broker"));
        try self.server_properties.put(try self.allocator.dupe(u8, "version"), try self.allocator.dupe(u8, "0.1.0"));
        try self.server_properties.put(try self.allocator.dupe(u8, "platform"), try self.allocator.dupe(u8, "Zig"));
        try self.server_properties.put(try self.allocator.dupe(u8, "copyright"), try self.allocator.dupe(u8, "Copyright (c) 2024 Cleverra Technologies"));
        try self.server_properties.put(try self.allocator.dupe(u8, "capabilities.basic.nack"), try self.allocator.dupe(u8, "true"));
        try self.server_properties.put(try self.allocator.dupe(u8, "capabilities.consumer_cancel_notify"), try self.allocator.dupe(u8, "true"));
        try self.server_properties.put(try self.allocator.dupe(u8, "capabilities.exchange_exchange_bindings"), try self.allocator.dupe(u8, "true"));
    }

    pub fn handleConnection(self: *ProtocolHandler, connection: *Connection) !void {
        // Send protocol header
        try self.sendProtocolHeader(connection);

        // Start connection handshake
        try self.sendConnectionStart(connection);
        connection.setState(.start_sent);

        // Main message processing loop
        while (!connection.isClosing()) {
            const frame = connection.receiveFrame() catch |err| {
                std.log.err("Connection {} frame receive error: {}", .{ connection.id, err });
                break;
            };

            if (frame) |f| {
                defer self.allocator.free(f.payload);
                try self.handleFrame(connection, f);
            } else {
                // Connection closed by client
                break;
            }

            // Check heartbeat
            if (!connection.checkHeartbeat()) {
                std.log.warn("Connection {} heartbeat timeout, closing", .{connection.id});
                break;
            }
        }

        std.log.info("Connection {} processing ended", .{connection.id});
    }

    fn sendProtocolHeader(self: *ProtocolHandler, connection: *Connection) !void {
        _ = self;

        // AMQP 0-9-1 protocol header: "AMQP" + [0, 0, 9, 1]
        const protocol_header = [_]u8{ 'A', 'M', 'Q', 'P', 0, 0, 9, 1 };

        const bytes_written = try connection.socket.write(&protocol_header);
        if (bytes_written != protocol_header.len) {
            return error.PartialWrite;
        }

        std.log.debug("Protocol header sent to connection {}", .{connection.id});
    }

    fn sendConnectionStart(self: *ProtocolHandler, connection: *Connection) !void {
        // Create Connection.Start method frame
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Connection = 10)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 10)));
        // Method ID (Start = 10)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 10)));

        // Version major (0)
        try payload.append(0);
        // Version minor (9)
        try payload.append(9);

        // Server properties (simplified - empty table for now)
        try payload.appendSlice(&std.mem.toBytes(@as(u32, 0))); // Empty field table

        // Mechanisms (PLAIN)
        const mechanisms = "PLAIN";
        try payload.appendSlice(&std.mem.toBytes(@as(u32, @intCast(mechanisms.len))));
        try payload.appendSlice(mechanisms);

        // Locales (en_US)
        const locales = "en_US";
        try payload.appendSlice(&std.mem.toBytes(@as(u32, @intCast(locales.len))));
        try payload.appendSlice(locales);

        const frame = Frame{
            .frame_type = .method,
            .channel = 0,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Connection.Start sent to connection {}", .{connection.id});
    }

    fn handleFrame(self: *ProtocolHandler, connection: *Connection, frame: Frame) !void {
        switch (frame.frame_type) {
            .method => try self.handleMethodFrame(connection, frame),
            .header => try self.handleHeaderFrame(connection, frame),
            .body => try self.handleBodyFrame(connection, frame),
            .heartbeat => try self.handleHeartbeatFrame(connection, frame),
        }
    }

    fn handleMethodFrame(self: *ProtocolHandler, connection: *Connection, frame: Frame) !void {
        if (frame.payload.len < 4) {
            return error.InvalidMethodFrame;
        }

        const class_id = std.mem.readInt(u16, frame.payload[0..2], .big);
        const method_id = std.mem.readInt(u16, frame.payload[2..4], .big);

        std.log.debug("Method frame: connection={}, channel={}, class={}, method={}", .{ connection.id, frame.channel, class_id, method_id });

        switch (class_id) {
            10 => try self.handleConnectionMethod(connection, method_id, frame.payload[4..]),
            20 => try self.handleChannelMethod(connection, frame.channel, method_id, frame.payload[4..]),
            40 => try self.handleExchangeMethod(connection, frame.channel, method_id, frame.payload[4..]),
            50 => try self.handleQueueMethod(connection, frame.channel, method_id, frame.payload[4..]),
            60 => try self.handleBasicMethod(connection, frame.channel, method_id, frame.payload[4..]),
            else => {
                std.log.warn("Unknown method class {} on connection {}", .{ class_id, connection.id });
                return error.UnknownMethodClass;
            },
        }
    }

    fn handleConnectionMethod(self: *ProtocolHandler, connection: *Connection, method_id: u16, payload: []const u8) !void {
        switch (method_id) {
            11 => try self.handleConnectionStartOk(connection, payload), // Start-Ok
            31 => try self.handleConnectionTuneOk(connection, payload), // Tune-Ok
            40 => try self.handleConnectionOpen(connection, payload), // Open
            50 => try self.handleConnectionClose(connection, payload), // Close
            else => {
                std.log.warn("Unknown connection method {} on connection {}", .{ method_id, connection.id });
                return error.UnknownConnectionMethod;
            },
        }
    }

    fn handleConnectionStartOk(self: *ProtocolHandler, connection: *Connection, payload: []const u8) !void {
        _ = payload; // TODO: Parse client properties, mechanism, response, locale

        connection.setState(.start_ok_received);

        // Send Connection.Tune
        try self.sendConnectionTune(connection);
        connection.setState(.tune_sent);

        std.log.debug("Connection.StartOk handled for connection {}", .{connection.id});
    }

    fn sendConnectionTune(self: *ProtocolHandler, connection: *Connection) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Connection = 10)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 10)));
        // Method ID (Tune = 30)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 30)));

        // Channel max
        try payload.appendSlice(&std.mem.toBytes(@as(u16, connection.channel_max)));
        // Frame max
        try payload.appendSlice(&std.mem.toBytes(@as(u32, connection.max_frame_size)));
        // Heartbeat
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 60))); // 60 seconds default

        const frame = Frame{
            .frame_type = .method,
            .channel = 0,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Connection.Tune sent to connection {}", .{connection.id});
    }

    fn handleConnectionTuneOk(self: *ProtocolHandler, connection: *Connection, payload: []const u8) !void {
        _ = self;

        if (payload.len < 8) {
            return error.InvalidTuneOk;
        }

        const channel_max = std.mem.readInt(u16, payload[0..2], .big);
        const frame_max = std.mem.readInt(u32, payload[2..6], .big);
        const heartbeat = std.mem.readInt(u16, payload[6..8], .big);

        // Update connection properties
        connection.channel_max = @min(connection.channel_max, channel_max);
        connection.max_frame_size = @min(connection.max_frame_size, frame_max);
        connection.heartbeat_interval = heartbeat;

        connection.setState(.tune_ok_received);

        std.log.debug("Connection.TuneOk handled for connection {}: channels={}, frame_size={}, heartbeat={}", .{ connection.id, connection.channel_max, connection.max_frame_size, connection.heartbeat_interval });
    }

    fn handleConnectionOpen(self: *ProtocolHandler, connection: *Connection, payload: []const u8) !void {
        if (payload.len < 1) {
            return error.InvalidConnectionOpen;
        }

        const vhost_len = payload[0];
        if (payload.len < 1 + vhost_len) {
            return error.InvalidConnectionOpen;
        }

        const vhost_name = payload[1 .. 1 + vhost_len];

        // Set virtual host
        try connection.setVirtualHost(vhost_name);
        connection.setState(.open_received);

        // Send Connection.OpenOk
        try self.sendConnectionOpenOk(connection);
        connection.setState(.open_ok_sent);
        connection.setState(.open);

        std.log.debug("Connection.Open handled for connection {}: vhost={s}", .{ connection.id, vhost_name });
    }

    fn sendConnectionOpenOk(self: *ProtocolHandler, connection: *Connection) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Connection = 10)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 10)));
        // Method ID (OpenOk = 41)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 41)));

        // Reserved field (empty short string)
        try payload.append(0);

        const frame = Frame{
            .frame_type = .method,
            .channel = 0,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Connection.OpenOk sent to connection {}", .{connection.id});
    }

    fn handleConnectionClose(self: *ProtocolHandler, connection: *Connection, payload: []const u8) !void {
        _ = payload; // TODO: Parse close reason

        connection.setState(.closing);

        // Send Connection.CloseOk
        try self.sendConnectionCloseOk(connection);
        connection.setState(.closed);

        std.log.debug("Connection.Close handled for connection {}", .{connection.id});
    }

    fn sendConnectionCloseOk(self: *ProtocolHandler, connection: *Connection) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Connection = 10)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 10)));
        // Method ID (CloseOk = 51)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 51)));

        const frame = Frame{
            .frame_type = .method,
            .channel = 0,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Connection.CloseOk sent to connection {}", .{connection.id});
    }

    fn handleChannelMethod(self: *ProtocolHandler, connection: *Connection, channel_id: u16, method_id: u16, payload: []const u8) !void {
        _ = self;
        _ = connection;
        _ = channel_id;
        _ = method_id;
        _ = payload;

        // TODO: Implement channel methods (Open, Close, Flow, etc.)
        std.log.debug("Channel method not yet implemented: channel={}, method={}", .{ channel_id, method_id });
    }

    fn handleExchangeMethod(self: *ProtocolHandler, connection: *Connection, channel_id: u16, method_id: u16, payload: []const u8) !void {
        _ = self;
        _ = connection;
        _ = channel_id;
        _ = method_id;
        _ = payload;

        // TODO: Implement exchange methods (Declare, Delete, Bind, etc.)
        std.log.debug("Exchange method not yet implemented: channel={}, method={}", .{ channel_id, method_id });
    }

    fn handleQueueMethod(self: *ProtocolHandler, connection: *Connection, channel_id: u16, method_id: u16, payload: []const u8) !void {
        _ = self;
        _ = connection;
        _ = channel_id;
        _ = method_id;
        _ = payload;

        // TODO: Implement queue methods (Declare, Delete, Bind, Purge, etc.)
        std.log.debug("Queue method not yet implemented: channel={}, method={}", .{ channel_id, method_id });
    }

    fn handleBasicMethod(self: *ProtocolHandler, connection: *Connection, channel_id: u16, method_id: u16, payload: []const u8) !void {
        _ = self;
        _ = connection;
        _ = channel_id;
        _ = method_id;
        _ = payload;

        // TODO: Implement basic methods (Publish, Consume, Ack, Nack, etc.)
        std.log.debug("Basic method not yet implemented: channel={}, method={}", .{ channel_id, method_id });
    }

    fn handleHeaderFrame(self: *ProtocolHandler, connection: *Connection, frame: Frame) !void {
        _ = self;
        _ = connection;
        _ = frame;

        // TODO: Implement content header handling
        std.log.debug("Header frame not yet implemented: connection={}, channel={}", .{ connection.id, frame.channel });
    }

    fn handleBodyFrame(self: *ProtocolHandler, connection: *Connection, frame: Frame) !void {
        _ = self;
        _ = connection;
        _ = frame;

        // TODO: Implement content body handling
        std.log.debug("Body frame not yet implemented: connection={}, channel={}", .{ connection.id, frame.channel });
    }

    fn handleHeartbeatFrame(self: *ProtocolHandler, connection: *Connection, frame: Frame) !void {
        _ = self;
        _ = frame;

        // Update last heartbeat time
        connection.last_heartbeat = std.time.timestamp();

        // Send heartbeat response
        try connection.sendHeartbeat();

        std.log.debug("Heartbeat handled for connection {}", .{connection.id});
    }
};

test "protocol handler creation" {
    const allocator = std.testing.allocator;

    var handler = ProtocolHandler.init(allocator);
    defer handler.deinit();

    try std.testing.expect(handler.server_properties.count() > 0);

    // Check that server properties are set
    try std.testing.expect(handler.server_properties.contains("product"));
    try std.testing.expect(handler.server_properties.contains("version"));
}
