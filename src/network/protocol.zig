const std = @import("std");
const Connection = @import("connection.zig").Connection;
const ConnectionState = @import("connection.zig").ConnectionState;
const Frame = @import("../protocol/frame.zig").Frame;
const FrameType = @import("../protocol/frame.zig").FrameType;
const Method = @import("../protocol/methods.zig").Method;
const MethodClass = @import("../protocol/methods.zig").MethodClass;
const ContentHeader = @import("../protocol/methods.zig").ContentHeader;
const BasicProperties = @import("../protocol/methods.zig").BasicProperties;
const VirtualHost = @import("../core/vhost.zig").VirtualHost;
const ExchangeType = @import("../routing/exchange.zig").ExchangeType;

// Structure to track messages in progress (Basic.Publish -> Content Header -> Content Body)
const PendingMessage = struct {
    exchange_name: []const u8,
    routing_key: []const u8,
    mandatory: bool,
    immediate: bool,
    header: ?ContentHeader,
    body_data: std.ArrayList(u8),
    expected_body_size: u64,
    
    pub fn init(allocator: std.mem.Allocator, exchange_name: []const u8, routing_key: []const u8, mandatory: bool, immediate: bool) !PendingMessage {
        return PendingMessage{
            .exchange_name = try allocator.dupe(u8, exchange_name),
            .routing_key = try allocator.dupe(u8, routing_key),
            .mandatory = mandatory,
            .immediate = immediate,
            .header = null,
            .body_data = std.ArrayList(u8).init(allocator),
            .expected_body_size = 0,
        };
    }
    
    pub fn deinit(self: *PendingMessage, allocator: std.mem.Allocator) void {
        allocator.free(self.exchange_name);
        allocator.free(self.routing_key);
        if (self.header) |*header| {
            header.deinit(allocator);
        }
        self.body_data.deinit();
    }
};

pub const ProtocolHandler = struct {
    allocator: std.mem.Allocator,
    server_properties: std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    get_vhost_fn: ?*const fn (vhost_name: []const u8) ?*VirtualHost,
    pending_messages: std.HashMap(u64, PendingMessage, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage), // channel_id -> PendingMessage

    pub fn init(allocator: std.mem.Allocator) ProtocolHandler {
        var handler = ProtocolHandler{
            .allocator = allocator,
            .server_properties = std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .get_vhost_fn = null,
            .pending_messages = std.HashMap(u64, PendingMessage, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage).init(allocator),
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
        
        // Clean up pending messages
        var pending_iterator = self.pending_messages.iterator();
        while (pending_iterator.next()) |entry| {
            entry.value_ptr.*.deinit(self.allocator);
        }
        self.pending_messages.deinit();
    }

    pub fn setVirtualHostGetter(self: *ProtocolHandler, get_vhost_fn: *const fn (vhost_name: []const u8) ?*VirtualHost) void {
        self.get_vhost_fn = get_vhost_fn;
    }

    fn getVirtualHost(self: *ProtocolHandler, connection: *Connection) ?*VirtualHost {
        const vhost_name = connection.virtual_host orelse return null;
        const get_fn = self.get_vhost_fn orelse return null;
        return get_fn(vhost_name);
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

        // Server properties (AMQP field table)
        const properties_data = try self.encodeFieldTable(self.server_properties);
        defer self.allocator.free(properties_data);
        try payload.appendSlice(&std.mem.toBytes(@as(u32, @intCast(properties_data.len))));
        try payload.appendSlice(properties_data);

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
            .channel_id = 0,
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

        std.log.debug("Method frame: connection={}, channel={}, class={}, method={}", .{ connection.id, frame.channel_id, class_id, method_id });

        switch (class_id) {
            10 => try self.handleConnectionMethod(connection, method_id, frame.payload[4..]),
            20 => try self.handleChannelMethod(connection, frame.channel_id, method_id, frame.payload[4..]),
            40 => try self.handleExchangeMethod(connection, frame.channel_id, method_id, frame.payload[4..]),
            50 => try self.handleQueueMethod(connection, frame.channel_id, method_id, frame.payload[4..]),
            60 => try self.handleBasicMethod(connection, frame.channel_id, method_id, frame.payload[4..]),
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
            .channel_id = 0,
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
            .channel_id = 0,
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
            .channel_id = 0,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Connection.CloseOk sent to connection {}", .{connection.id});
    }

    fn handleChannelOpen(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = payload; // Channel.Open has no arguments in AMQP 0-9-1

        // Validate channel ID (0 is reserved for connection-level operations)
        if (channel_id == 0) {
            std.log.warn("Invalid channel ID 0 for Channel.Open on connection {}", .{connection.id});
            return error.InvalidChannelId;
        }

        // Check if connection is open
        if (!connection.isOpen()) {
            std.log.warn("Channel.Open on non-open connection {}", .{connection.id});
            return error.ConnectionNotOpen;
        }

        // Create the channel
        connection.addChannel(channel_id) catch |err| switch (err) {
            error.ChannelAlreadyExists => {
                std.log.warn("Channel {} already exists on connection {}", .{ channel_id, connection.id });
                return error.ChannelAlreadyExists;
            },
            else => return err,
        };

        // Send Channel.OpenOk
        try self.sendChannelOpenOk(connection, channel_id);

        std.log.debug("Channel.Open handled for connection {}, channel {}", .{ connection.id, channel_id });
    }

    fn sendChannelOpenOk(self: *ProtocolHandler, connection: *Connection, channel_id: u16) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Channel = 20)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 20)));
        // Method ID (OpenOk = 11)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 11)));

        // Reserved field (empty long string)
        try payload.appendSlice(&std.mem.toBytes(@as(u32, 0)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Channel.OpenOk sent to connection {}, channel {}", .{ connection.id, channel_id });
    }

    fn handleChannelFlow(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        if (payload.len < 1) {
            return error.InvalidChannelFlow;
        }

        const active = (payload[0] & 0x01) != 0;

        // Get the channel
        const channel = connection.getChannel(channel_id) orelse {
            std.log.warn("Channel.Flow on non-existent channel {} on connection {}", .{ channel_id, connection.id });
            return error.ChannelNotFound;
        };

        // Update flow control state
        channel.flow_active = active;

        // Send Channel.FlowOk
        try self.sendChannelFlowOk(connection, channel_id, active);

        std.log.debug("Channel.Flow handled for connection {}, channel {}: active={}", .{ connection.id, channel_id, active });
    }

    fn sendChannelFlowOk(self: *ProtocolHandler, connection: *Connection, channel_id: u16, active: bool) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Channel = 20)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 20)));
        // Method ID (FlowOk = 21)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 21)));

        // Active flag
        try payload.append(if (active) 1 else 0);

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Channel.FlowOk sent to connection {}, channel {}: active={}", .{ connection.id, channel_id, active });
    }

    fn handleChannelClose(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = payload; // TODO: Parse close reply code, reply text, class id, method id

        // Get the channel to ensure it exists
        const channel = connection.getChannel(channel_id) orelse {
            std.log.warn("Channel.Close on non-existent channel {} on connection {}", .{ channel_id, connection.id });
            return error.ChannelNotFound;
        };

        // Mark channel as inactive
        channel.active = false;

        // Send Channel.CloseOk
        try self.sendChannelCloseOk(connection, channel_id);

        // Remove the channel
        connection.removeChannel(channel_id);

        std.log.debug("Channel.Close handled for connection {}, channel {}", .{ connection.id, channel_id });
    }

    fn sendChannelCloseOk(self: *ProtocolHandler, connection: *Connection, channel_id: u16) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Channel = 20)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 20)));
        // Method ID (CloseOk = 41)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 41)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Channel.CloseOk sent to connection {}, channel {}", .{ connection.id, channel_id });
    }

    fn handleChannelMethod(self: *ProtocolHandler, connection: *Connection, channel_id: u16, method_id: u16, payload: []const u8) !void {
        switch (method_id) {
            10 => try self.handleChannelOpen(connection, channel_id, payload), // Open
            20 => try self.handleChannelFlow(connection, channel_id, payload), // Flow
            40 => try self.handleChannelClose(connection, channel_id, payload), // Close
            else => {
                std.log.warn("Unknown channel method {} on connection {}, channel {}", .{ method_id, connection.id, channel_id });
                return error.UnknownChannelMethod;
            },
        }
    }

    fn handleExchangeMethod(self: *ProtocolHandler, connection: *Connection, channel_id: u16, method_id: u16, payload: []const u8) !void {
        switch (method_id) {
            10 => try self.handleExchangeDeclare(connection, channel_id, payload), // Declare
            20 => try self.handleExchangeDelete(connection, channel_id, payload), // Delete
            30 => try self.handleExchangeBind(connection, channel_id, payload), // Bind
            40 => try self.handleExchangeUnbind(connection, channel_id, payload), // Unbind
            else => {
                std.log.warn("Unknown exchange method {} on connection {}, channel {}", .{ method_id, connection.id, channel_id });
                return error.UnknownExchangeMethod;
            },
        }
    }

    fn handleExchangeDeclare(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        if (payload.len < 3) {
            return error.InvalidExchangeDeclare;
        }

        var stream = std.io.fixedBufferStream(payload);
        const reader = stream.reader();

        // Skip reserved field
        _ = try reader.readInt(u16, .big);

        // Read exchange name
        const name_len = try reader.readInt(u8, .big);
        var name_buf: [256]u8 = undefined;
        if (name_len > name_buf.len) return error.ExchangeNameTooLong;
        try reader.readNoEof(name_buf[0..name_len]);
        const exchange_name = name_buf[0..name_len];

        // Read exchange type
        const type_len = try reader.readInt(u8, .big);
        var type_buf: [16]u8 = undefined;
        if (type_len > type_buf.len) return error.ExchangeTypeTooLong;
        try reader.readNoEof(type_buf[0..type_len]);
        const type_str = type_buf[0..type_len];

        // Parse exchange type
        const exchange_type = if (std.mem.eql(u8, type_str, "direct"))
            ExchangeType.direct
        else if (std.mem.eql(u8, type_str, "fanout"))
            ExchangeType.fanout
        else if (std.mem.eql(u8, type_str, "topic"))
            ExchangeType.topic
        else if (std.mem.eql(u8, type_str, "headers"))
            ExchangeType.headers
        else
            return error.UnsupportedExchangeType;

        // Read flags
        const flags = try reader.readInt(u8, .big);
        const passive = (flags & 0x01) != 0;
        const durable = (flags & 0x02) != 0;
        const auto_delete = (flags & 0x04) != 0;
        const internal = (flags & 0x08) != 0;
        const no_wait = (flags & 0x10) != 0;

        // Read arguments (field table)
        const args_len = try reader.readInt(u32, .big);
        var arguments: ?[]const u8 = null;
        if (args_len > 0) {
            const args_buf = try self.allocator.alloc(u8, args_len);
            try reader.readNoEof(args_buf);
            arguments = args_buf;
        }
        defer if (arguments) |args| self.allocator.free(args);

        // Get virtual host
        const vhost = self.getVirtualHost(connection) orelse {
            std.log.warn("Exchange.Declare on connection {} with no virtual host", .{connection.id});
            return error.NoVirtualHost;
        };

        // Check if channel exists
        const channel = connection.getChannel(channel_id) orelse {
            std.log.warn("Exchange.Declare on non-existent channel {} on connection {}", .{ channel_id, connection.id });
            return error.ChannelNotFound;
        };

        if (!channel.active) {
            return error.ChannelNotActive;
        }

        // Declare exchange
        if (passive) {
            // Passive declare - just check if exchange exists
            if (vhost.getExchange(exchange_name) == null) {
                return error.ExchangeNotFound;
            }
        } else {
            // Active declare - create exchange
            vhost.declareExchange(exchange_name, exchange_type, durable, auto_delete, internal, arguments) catch |err| switch (err) {
                error.ExchangeParameterMismatch => return error.ExchangeParameterMismatch,
                else => return err,
            };
        }

        // Send Exchange.DeclareOk if not no_wait
        if (!no_wait) {
            try self.sendExchangeDeclareOk(connection, channel_id);
        }

        std.log.debug("Exchange.Declare handled for connection {}, channel {}: {s} (type: {s})", .{ connection.id, channel_id, exchange_name, @tagName(exchange_type) });
    }

    fn sendExchangeDeclareOk(self: *ProtocolHandler, connection: *Connection, channel_id: u16) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Exchange = 40)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 40)));
        // Method ID (DeclareOk = 11)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 11)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Exchange.DeclareOk sent to connection {}, channel {}", .{ connection.id, channel_id });
    }

    fn handleExchangeDelete(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        if (payload.len < 4) {
            return error.InvalidExchangeDelete;
        }

        var stream = std.io.fixedBufferStream(payload);
        const reader = stream.reader();

        // Skip reserved field
        _ = try reader.readInt(u16, .big);

        // Read exchange name
        const name_len = try reader.readInt(u8, .big);
        var name_buf: [256]u8 = undefined;
        if (name_len > name_buf.len) return error.ExchangeNameTooLong;
        try reader.readNoEof(name_buf[0..name_len]);
        const exchange_name = name_buf[0..name_len];

        // Read flags
        const flags = try reader.readInt(u8, .big);
        const if_unused = (flags & 0x01) != 0;
        const no_wait = (flags & 0x02) != 0;

        // Get virtual host
        const vhost = self.getVirtualHost(connection) orelse {
            std.log.warn("Exchange.Delete on connection {} with no virtual host", .{connection.id});
            return error.NoVirtualHost;
        };

        // Check if channel exists
        const channel = connection.getChannel(channel_id) orelse {
            std.log.warn("Exchange.Delete on non-existent channel {} on connection {}", .{ channel_id, connection.id });
            return error.ChannelNotFound;
        };

        if (!channel.active) {
            return error.ChannelNotActive;
        }

        // Delete exchange
        vhost.deleteExchange(exchange_name, if_unused) catch |err| switch (err) {
            error.ExchangeNotFound => return error.ExchangeNotFound,
            error.ExchangeInUse => return error.ExchangeInUse,
            else => return err,
        };

        // Send Exchange.DeleteOk if not no_wait
        if (!no_wait) {
            try self.sendExchangeDeleteOk(connection, channel_id);
        }

        std.log.debug("Exchange.Delete handled for connection {}, channel {}: {s}", .{ connection.id, channel_id, exchange_name });
    }

    fn sendExchangeDeleteOk(self: *ProtocolHandler, connection: *Connection, channel_id: u16) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Exchange = 40)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 40)));
        // Method ID (DeleteOk = 21)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 21)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Exchange.DeleteOk sent to connection {}, channel {}", .{ connection.id, channel_id });
    }

    fn handleExchangeBind(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = self;
        _ = connection;
        _ = channel_id;
        _ = payload;

        // TODO: Exchange-to-exchange binding is not widely used in AMQP 0-9-1
        // For now, return an error indicating this is not supported
        std.log.debug("Exchange.Bind not implemented: exchange-to-exchange binding not supported", .{});
        return error.ExchangeBindNotSupported;
    }

    fn handleExchangeUnbind(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = self;
        _ = connection;
        _ = channel_id;
        _ = payload;

        // TODO: Exchange-to-exchange unbinding is not widely used in AMQP 0-9-1
        // For now, return an error indicating this is not supported
        std.log.debug("Exchange.Unbind not implemented: exchange-to-exchange unbinding not supported", .{});
        return error.ExchangeUnbindNotSupported;
    }

    fn handleQueueMethod(self: *ProtocolHandler, connection: *Connection, channel_id: u16, method_id: u16, payload: []const u8) !void {
        switch (method_id) {
            10 => try self.handleQueueDeclare(connection, channel_id, payload), // Declare
            20 => try self.handleQueueBind(connection, channel_id, payload), // Bind
            30 => try self.handleQueuePurge(connection, channel_id, payload), // Purge
            40 => try self.handleQueueDelete(connection, channel_id, payload), // Delete
            50 => try self.handleQueueUnbind(connection, channel_id, payload), // Unbind
            else => {
                std.log.warn("Unknown queue method {} on connection {}, channel {}", .{ method_id, connection.id, channel_id });
                return error.UnknownQueueMethod;
            },
        }
    }

    fn handleQueueDeclare(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        if (payload.len < 3) {
            return error.InvalidQueueDeclare;
        }

        var stream = std.io.fixedBufferStream(payload);
        const reader = stream.reader();

        // Skip reserved field
        _ = try reader.readInt(u16, .big);

        // Read queue name
        const name_len = try reader.readInt(u8, .big);
        var name_buf: [256]u8 = undefined;
        if (name_len > name_buf.len) return error.QueueNameTooLong;
        try reader.readNoEof(name_buf[0..name_len]);
        const queue_name = name_buf[0..name_len];

        // Read flags
        const flags = try reader.readInt(u8, .big);
        const passive = (flags & 0x01) != 0;
        const durable = (flags & 0x02) != 0;
        const exclusive = (flags & 0x04) != 0;
        const auto_delete = (flags & 0x08) != 0;
        const no_wait = (flags & 0x10) != 0;

        // Read arguments (field table)
        const args_len = try reader.readInt(u32, .big);
        var arguments: ?[]const u8 = null;
        if (args_len > 0) {
            const args_buf = try self.allocator.alloc(u8, args_len);
            try reader.readNoEof(args_buf);
            arguments = args_buf;
        }
        defer if (arguments) |args| self.allocator.free(args);

        // Get virtual host
        const vhost = self.getVirtualHost(connection) orelse {
            std.log.warn("Queue.Declare on connection {} with no virtual host", .{connection.id});
            return error.NoVirtualHost;
        };

        // Check if channel exists
        const channel = connection.getChannel(channel_id) orelse {
            std.log.warn("Queue.Declare on non-existent channel {} on connection {}", .{ channel_id, connection.id });
            return error.ChannelNotFound;
        };

        if (!channel.active) {
            return error.ChannelNotActive;
        }

        // Declare queue
        var actual_queue_name: []const u8 = undefined;
        if (passive) {
            // Passive declare - just check if queue exists
            if (vhost.getQueue(queue_name) == null) {
                return error.QueueNotFound;
            }
            actual_queue_name = queue_name;
        } else {
            // Active declare - create queue
            actual_queue_name = vhost.declareQueue(queue_name, durable, exclusive, auto_delete, arguments) catch |err| switch (err) {
                error.QueueParameterMismatch => return error.QueueParameterMismatch,
                else => return err,
            };
        }

        // Send Queue.DeclareOk if not no_wait
        if (!no_wait) {
            try self.sendQueueDeclareOk(connection, channel_id, actual_queue_name);
        }

        std.log.debug("Queue.Declare handled for connection {}, channel {}: {s}", .{ connection.id, channel_id, actual_queue_name });
    }

    fn sendQueueDeclareOk(self: *ProtocolHandler, connection: *Connection, channel_id: u16, queue_name: []const u8) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Queue = 50)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 50)));
        // Method ID (DeclareOk = 11)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 11)));

        // Queue name
        try payload.append(@intCast(queue_name.len));
        try payload.appendSlice(queue_name);

        // Message count (placeholder - would need actual count from queue)
        try payload.appendSlice(&std.mem.toBytes(@as(u32, 0)));

        // Consumer count (placeholder - would need actual count from queue)
        try payload.appendSlice(&std.mem.toBytes(@as(u32, 0)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Queue.DeclareOk sent to connection {}, channel {}: {s}", .{ connection.id, channel_id, queue_name });
    }

    fn handleQueueBind(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        if (payload.len < 4) {
            return error.InvalidQueueBind;
        }

        var stream = std.io.fixedBufferStream(payload);
        const reader = stream.reader();

        // Skip reserved field
        _ = try reader.readInt(u16, .big);

        // Read queue name
        const queue_name_len = try reader.readInt(u8, .big);
        var queue_name_buf: [256]u8 = undefined;
        if (queue_name_len > queue_name_buf.len) return error.QueueNameTooLong;
        try reader.readNoEof(queue_name_buf[0..queue_name_len]);
        const queue_name = queue_name_buf[0..queue_name_len];

        // Read exchange name
        const exchange_name_len = try reader.readInt(u8, .big);
        var exchange_name_buf: [256]u8 = undefined;
        if (exchange_name_len > exchange_name_buf.len) return error.ExchangeNameTooLong;
        try reader.readNoEof(exchange_name_buf[0..exchange_name_len]);
        const exchange_name = exchange_name_buf[0..exchange_name_len];

        // Read routing key
        const routing_key_len = try reader.readInt(u8, .big);
        var routing_key_buf: [256]u8 = undefined;
        if (routing_key_len > routing_key_buf.len) return error.RoutingKeyTooLong;
        try reader.readNoEof(routing_key_buf[0..routing_key_len]);
        const routing_key = routing_key_buf[0..routing_key_len];

        // Read flags
        const flags = try reader.readInt(u8, .big);
        const no_wait = (flags & 0x01) != 0;

        // Read arguments (field table)
        const args_len = try reader.readInt(u32, .big);
        var arguments: ?[]const u8 = null;
        if (args_len > 0) {
            const args_buf = try self.allocator.alloc(u8, args_len);
            try reader.readNoEof(args_buf);
            arguments = args_buf;
        }
        defer if (arguments) |args| self.allocator.free(args);

        // Get virtual host
        const vhost = self.getVirtualHost(connection) orelse {
            std.log.warn("Queue.Bind on connection {} with no virtual host", .{connection.id});
            return error.NoVirtualHost;
        };

        // Check if channel exists
        const channel = connection.getChannel(channel_id) orelse {
            std.log.warn("Queue.Bind on non-existent channel {} on connection {}", .{ channel_id, connection.id });
            return error.ChannelNotFound;
        };

        if (!channel.active) {
            return error.ChannelNotActive;
        }

        // Bind queue to exchange
        vhost.bindQueue(queue_name, exchange_name, routing_key, arguments) catch |err| switch (err) {
            error.QueueNotFound => return error.QueueNotFound,
            error.ExchangeNotFound => return error.ExchangeNotFound,
            else => return err,
        };

        // Send Queue.BindOk if not no_wait
        if (!no_wait) {
            try self.sendQueueBindOk(connection, channel_id);
        }

        std.log.debug("Queue.Bind handled for connection {}, channel {}: {s} -> {s} (key: {s})", .{ connection.id, channel_id, exchange_name, queue_name, routing_key });
    }

    fn sendQueueBindOk(self: *ProtocolHandler, connection: *Connection, channel_id: u16) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Queue = 50)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 50)));
        // Method ID (BindOk = 21)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 21)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Queue.BindOk sent to connection {}, channel {}", .{ connection.id, channel_id });
    }

    fn handleQueueUnbind(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        if (payload.len < 4) {
            return error.InvalidQueueUnbind;
        }

        var stream = std.io.fixedBufferStream(payload);
        const reader = stream.reader();

        // Skip reserved field
        _ = try reader.readInt(u16, .big);

        // Read queue name
        const queue_name_len = try reader.readInt(u8, .big);
        var queue_name_buf: [256]u8 = undefined;
        if (queue_name_len > queue_name_buf.len) return error.QueueNameTooLong;
        try reader.readNoEof(queue_name_buf[0..queue_name_len]);
        const queue_name = queue_name_buf[0..queue_name_len];

        // Read exchange name
        const exchange_name_len = try reader.readInt(u8, .big);
        var exchange_name_buf: [256]u8 = undefined;
        if (exchange_name_len > exchange_name_buf.len) return error.ExchangeNameTooLong;
        try reader.readNoEof(exchange_name_buf[0..exchange_name_len]);
        const exchange_name = exchange_name_buf[0..exchange_name_len];

        // Read routing key
        const routing_key_len = try reader.readInt(u8, .big);
        var routing_key_buf: [256]u8 = undefined;
        if (routing_key_len > routing_key_buf.len) return error.RoutingKeyTooLong;
        try reader.readNoEof(routing_key_buf[0..routing_key_len]);
        const routing_key = routing_key_buf[0..routing_key_len];

        // Read arguments (field table)
        const args_len = try reader.readInt(u32, .big);
        var arguments: ?[]const u8 = null;
        if (args_len > 0) {
            const args_buf = try self.allocator.alloc(u8, args_len);
            try reader.readNoEof(args_buf);
            arguments = args_buf;
        }
        defer if (arguments) |args| self.allocator.free(args);

        // Get virtual host
        const vhost = self.getVirtualHost(connection) orelse {
            std.log.warn("Queue.Unbind on connection {} with no virtual host", .{connection.id});
            return error.NoVirtualHost;
        };

        // Check if channel exists
        const channel = connection.getChannel(channel_id) orelse {
            std.log.warn("Queue.Unbind on non-existent channel {} on connection {}", .{ channel_id, connection.id });
            return error.ChannelNotFound;
        };

        if (!channel.active) {
            return error.ChannelNotActive;
        }

        // Unbind queue from exchange
        vhost.unbindQueue(queue_name, exchange_name, routing_key, arguments) catch |err| switch (err) {
            error.QueueNotFound => return error.QueueNotFound,
            error.ExchangeNotFound => return error.ExchangeNotFound,
            else => return err,
        };

        // Send Queue.UnbindOk
        try self.sendQueueUnbindOk(connection, channel_id);

        std.log.debug("Queue.Unbind handled for connection {}, channel {}: {s} -/-> {s} (key: {s})", .{ connection.id, channel_id, exchange_name, queue_name, routing_key });
    }

    fn sendQueueUnbindOk(self: *ProtocolHandler, connection: *Connection, channel_id: u16) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Queue = 50)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 50)));
        // Method ID (UnbindOk = 51)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 51)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Queue.UnbindOk sent to connection {}, channel {}", .{ connection.id, channel_id });
    }

    fn handleQueuePurge(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        if (payload.len < 3) {
            return error.InvalidQueuePurge;
        }

        var stream = std.io.fixedBufferStream(payload);
        const reader = stream.reader();

        // Skip reserved field
        _ = try reader.readInt(u16, .big);

        // Read queue name
        const name_len = try reader.readInt(u8, .big);
        var name_buf: [256]u8 = undefined;
        if (name_len > name_buf.len) return error.QueueNameTooLong;
        try reader.readNoEof(name_buf[0..name_len]);
        const queue_name = name_buf[0..name_len];

        // Read flags
        const flags = try reader.readInt(u8, .big);
        const no_wait = (flags & 0x01) != 0;

        // Get virtual host
        const vhost = self.getVirtualHost(connection) orelse {
            std.log.warn("Queue.Purge on connection {} with no virtual host", .{connection.id});
            return error.NoVirtualHost;
        };

        // Check if channel exists
        const channel = connection.getChannel(channel_id) orelse {
            std.log.warn("Queue.Purge on non-existent channel {} on connection {}", .{ channel_id, connection.id });
            return error.ChannelNotFound;
        };

        if (!channel.active) {
            return error.ChannelNotActive;
        }

        // Purge queue
        const message_count = vhost.purgeQueue(queue_name) catch |err| switch (err) {
            error.QueueNotFound => return error.QueueNotFound,
            else => return err,
        };

        // Send Queue.PurgeOk if not no_wait
        if (!no_wait) {
            try self.sendQueuePurgeOk(connection, channel_id, message_count);
        }

        std.log.debug("Queue.Purge handled for connection {}, channel {}: {s} ({} messages)", .{ connection.id, channel_id, queue_name, message_count });
    }

    fn sendQueuePurgeOk(self: *ProtocolHandler, connection: *Connection, channel_id: u16, message_count: u32) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Queue = 50)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 50)));
        // Method ID (PurgeOk = 31)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 31)));

        // Message count
        try payload.appendSlice(&std.mem.toBytes(message_count));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Queue.PurgeOk sent to connection {}, channel {}: {} messages", .{ connection.id, channel_id, message_count });
    }

    fn handleQueueDelete(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        if (payload.len < 3) {
            return error.InvalidQueueDelete;
        }

        var stream = std.io.fixedBufferStream(payload);
        const reader = stream.reader();

        // Skip reserved field
        _ = try reader.readInt(u16, .big);

        // Read queue name
        const name_len = try reader.readInt(u8, .big);
        var name_buf: [256]u8 = undefined;
        if (name_len > name_buf.len) return error.QueueNameTooLong;
        try reader.readNoEof(name_buf[0..name_len]);
        const queue_name = name_buf[0..name_len];

        // Read flags
        const flags = try reader.readInt(u8, .big);
        const if_unused = (flags & 0x01) != 0;
        const if_empty = (flags & 0x02) != 0;
        const no_wait = (flags & 0x04) != 0;

        // Get virtual host
        const vhost = self.getVirtualHost(connection) orelse {
            std.log.warn("Queue.Delete on connection {} with no virtual host", .{connection.id});
            return error.NoVirtualHost;
        };

        // Check if channel exists
        const channel = connection.getChannel(channel_id) orelse {
            std.log.warn("Queue.Delete on non-existent channel {} on connection {}", .{ channel_id, connection.id });
            return error.ChannelNotFound;
        };

        if (!channel.active) {
            return error.ChannelNotActive;
        }

        // Delete queue
        const message_count = vhost.deleteQueue(queue_name, if_unused, if_empty) catch |err| switch (err) {
            error.QueueNotFound => return error.QueueNotFound,
            error.QueueInUse => return error.QueueInUse,
            error.QueueNotEmpty => return error.QueueNotEmpty,
            else => return err,
        };

        // Send Queue.DeleteOk if not no_wait
        if (!no_wait) {
            try self.sendQueueDeleteOk(connection, channel_id, message_count);
        }

        std.log.debug("Queue.Delete handled for connection {}, channel {}: {s} ({} messages)", .{ connection.id, channel_id, queue_name, message_count });
    }

    fn sendQueueDeleteOk(self: *ProtocolHandler, connection: *Connection, channel_id: u16, message_count: u32) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Queue = 50)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 50)));
        // Method ID (DeleteOk = 41)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 41)));

        // Message count
        try payload.appendSlice(&std.mem.toBytes(message_count));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Queue.DeleteOk sent to connection {}, channel {}: {} messages", .{ connection.id, channel_id, message_count });
    }

    fn handleBasicMethod(self: *ProtocolHandler, connection: *Connection, channel_id: u16, method_id: u16, payload: []const u8) !void {
        switch (method_id) {
            10 => try self.handleBasicQos(connection, channel_id, payload), // QoS
            20 => try self.handleBasicConsume(connection, channel_id, payload), // Consume
            30 => try self.handleBasicCancel(connection, channel_id, payload), // Cancel
            40 => try self.handleBasicPublish(connection, channel_id, payload), // Publish
            50 => try self.handleBasicReturn(connection, channel_id, payload), // Return
            60 => try self.handleBasicDeliver(connection, channel_id, payload), // Deliver
            70 => try self.handleBasicGet(connection, channel_id, payload), // Get
            80 => try self.handleBasicAck(connection, channel_id, payload), // Ack
            90 => try self.handleBasicReject(connection, channel_id, payload), // Reject
            100 => try self.handleBasicRecoverAsync(connection, channel_id, payload), // RecoverAsync
            110 => try self.handleBasicRecover(connection, channel_id, payload), // Recover
            120 => try self.handleBasicNack(connection, channel_id, payload), // Nack
            else => {
                std.log.warn("Unknown basic method {} on channel {} connection {}", .{ method_id, channel_id, connection.id });
                return error.UnknownBasicMethod;
            },
        }
    }

    // Basic method implementations
    fn handleBasicQos(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = self;

        // Check if channel exists
        const channel = connection.getChannel(channel_id) orelse {
            std.log.warn("Basic.QoS on non-existent channel {} on connection {}", .{ channel_id, connection.id });
            return error.ChannelNotFound;
        };

        if (!channel.active) {
            return error.ChannelNotActive;
        }

        if (payload.len < 5) return error.InvalidBasicQos;

        var stream = std.io.fixedBufferStream(payload);
        const reader = stream.reader();

        const prefetch_size = try reader.readInt(u32, .big);
        const prefetch_count = try reader.readInt(u16, .big);
        const global = (try reader.readInt(u8, .big)) & 0x01 != 0;

        // Update channel QoS settings
        if (!global) {
            channel.prefetch_size = prefetch_size;
            channel.prefetch_count = prefetch_count;
        }

        // Send Basic.QosOk response
        const response_frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = &[_]u8{ 0x00, 0x3C, 0x00, 0x0B }, // class=60, method=11 (QosOk)
        };

        try connection.sendFrame(response_frame);
        std.log.debug("Basic.QosOk sent to connection {}, channel {}", .{ connection.id, channel_id });
    }

    fn handleBasicPublish(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        // Check if channel exists
        const channel = connection.getChannel(channel_id) orelse {
            std.log.warn("Basic.Publish on non-existent channel {} on connection {}", .{ channel_id, connection.id });
            return error.ChannelNotFound;
        };

        if (!channel.active) {
            return error.ChannelNotActive;
        }

        // Get virtual host
        const vhost = self.getVirtualHost(connection) orelse {
            std.log.warn("Basic.Publish on connection {} with no virtual host", .{connection.id});
            return error.NoVirtualHost;
        };

        if (payload.len < 3) return error.InvalidBasicPublish;

        var stream = std.io.fixedBufferStream(payload);
        const reader = stream.reader();

        // Parse Basic.Publish parameters
        _ = try reader.readInt(u16, .big); // reserved

        // Read exchange name
        const exchange_len = try reader.readInt(u8, .big);
        var exchange_buf: [256]u8 = undefined;
        if (exchange_len > exchange_buf.len) return error.ExchangeNameTooLong;
        try reader.readNoEof(exchange_buf[0..exchange_len]);
        const exchange_name = if (exchange_len == 0) "" else exchange_buf[0..exchange_len];

        // Read routing key
        const routing_key_len = try reader.readInt(u8, .big);
        var routing_key_buf: [256]u8 = undefined;
        if (routing_key_len > routing_key_buf.len) return error.RoutingKeyTooLong;
        try reader.readNoEof(routing_key_buf[0..routing_key_len]);
        const routing_key = routing_key_buf[0..routing_key_len];

        // Read flags
        const flags = try reader.readInt(u8, .big);
        const mandatory = (flags & 0x01) != 0;
        const immediate = (flags & 0x02) != 0;

        // Get exchange (use default exchange if empty name)
        const exchange = vhost.getExchange(exchange_name) orelse {
            std.log.warn("Basic.Publish to non-existent exchange '{s}' on connection {}", .{ exchange_name, connection.id });
            if (mandatory) {
                // TODO: Send Basic.Return for mandatory message to non-existent exchange
                return error.ExchangeNotFound;
            }
            return; // Silently drop non-mandatory message
        };

        // Create pending message to track content header and body frames
        const channel_key = (@as(u64, connection.id) << 16) | channel_id;
        
        // Remove any existing pending message for this channel
        if (self.pending_messages.fetchRemove(channel_key)) |existing| {
            var existing_msg = existing.value;
            existing_msg.deinit(self.allocator);
        }
        
        // Create new pending message
        const pending_message = try PendingMessage.init(self.allocator, exchange_name, routing_key, mandatory, immediate);
        try self.pending_messages.put(channel_key, pending_message);
        
        // TODO: Handle immediate flag
        _ = exchange; // Will be used when routing the complete message

        std.log.debug("Basic.Publish received: exchange={s}, routing_key={s}, mandatory={}, immediate={} - awaiting content", .{ exchange_name, routing_key, mandatory, immediate });
    }

    fn handleBasicConsume(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        // Check if channel exists
        const channel = connection.getChannel(channel_id) orelse {
            std.log.warn("Basic.Consume on non-existent channel {} on connection {}", .{ channel_id, connection.id });
            return error.ChannelNotFound;
        };

        if (!channel.active) {
            return error.ChannelNotActive;
        }

        // Get virtual host
        const vhost = self.getVirtualHost(connection) orelse {
            std.log.warn("Basic.Consume on connection {} with no virtual host", .{connection.id});
            return error.NoVirtualHost;
        };

        if (payload.len < 3) return error.InvalidBasicConsume;

        var stream = std.io.fixedBufferStream(payload);
        const reader = stream.reader();

        // Parse Basic.Consume parameters
        _ = try reader.readInt(u16, .big); // reserved

        // Read queue name
        const queue_len = try reader.readInt(u8, .big);
        var queue_buf: [256]u8 = undefined;
        if (queue_len > queue_buf.len) return error.QueueNameTooLong;
        try reader.readNoEof(queue_buf[0..queue_len]);
        const queue_name = queue_buf[0..queue_len];

        // Read consumer tag
        const consumer_tag_len = try reader.readInt(u8, .big);
        var consumer_tag_buf: [256]u8 = undefined;
        if (consumer_tag_len > consumer_tag_buf.len) return error.ConsumerTagTooLong;
        try reader.readNoEof(consumer_tag_buf[0..consumer_tag_len]);
        const consumer_tag = consumer_tag_buf[0..consumer_tag_len];

        // Read flags
        const flags = try reader.readInt(u8, .big);
        const no_local = (flags & 0x01) != 0;
        const no_ack = (flags & 0x02) != 0;
        const exclusive = (flags & 0x04) != 0;
        const no_wait = (flags & 0x08) != 0;

        // TODO: Read arguments field table (for now skip)

        // Get queue
        const queue = vhost.getQueue(queue_name) orelse {
            std.log.warn("Basic.Consume on non-existent queue '{s}' on connection {}", .{ queue_name, connection.id });
            return error.QueueNotFound;
        };

        _ = queue; // TODO: Register consumer with queue
        _ = no_local;
        _ = exclusive;

        if (!no_wait) {
            // Send Basic.ConsumeOk response
            var response_buf = std.ArrayList(u8).init(self.allocator);
            defer response_buf.deinit();

            try response_buf.writer().writeInt(u16, 60, .big); // class=60 (Basic)
            try response_buf.writer().writeInt(u16, 21, .big); // method=21 (ConsumeOk)

            // Consumer tag
            try response_buf.writer().writeInt(u8, @intCast(consumer_tag.len), .big);
            try response_buf.appendSlice(consumer_tag);

            const response_frame = Frame{
                .frame_type = .method,
                .channel_id = channel_id,
                .payload = try self.allocator.dupe(u8, response_buf.items),
            };
            defer self.allocator.free(response_frame.payload);

            try connection.sendFrame(response_frame);
            std.log.debug("Basic.ConsumeOk sent to connection {}, channel {}: consumer_tag={s}", .{ connection.id, channel_id, consumer_tag });
        }

        std.log.debug("Basic.Consume processed: queue={s}, consumer_tag={s}, no_ack={}", .{ queue_name, consumer_tag, no_ack });
    }

    fn handleBasicAck(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = self;

        // Check if channel exists
        const channel = connection.getChannel(channel_id) orelse {
            std.log.warn("Basic.Ack on non-existent channel {} on connection {}", .{ channel_id, connection.id });
            return error.ChannelNotFound;
        };

        if (!channel.active) {
            return error.ChannelNotActive;
        }

        if (payload.len < 9) return error.InvalidBasicAck;

        var stream = std.io.fixedBufferStream(payload);
        const reader = stream.reader();

        const delivery_tag = try reader.readInt(u64, .big);
        const multiple = (try reader.readInt(u8, .big)) & 0x01 != 0;

        // Acknowledge message(s)
        channel.ackMessage(delivery_tag, multiple);

        std.log.debug("Basic.Ack processed: delivery_tag={}, multiple={}", .{ delivery_tag, multiple });
    }

    fn handleBasicNack(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = self;

        // Check if channel exists
        const channel = connection.getChannel(channel_id) orelse {
            std.log.warn("Basic.Nack on non-existent channel {} on connection {}", .{ channel_id, connection.id });
            return error.ChannelNotFound;
        };

        if (!channel.active) {
            return error.ChannelNotActive;
        }

        if (payload.len < 10) return error.InvalidBasicNack;

        var stream = std.io.fixedBufferStream(payload);
        const reader = stream.reader();

        const delivery_tag = try reader.readInt(u64, .big);
        const flags = try reader.readInt(u8, .big);
        const multiple = (flags & 0x01) != 0;
        const requeue = (flags & 0x02) != 0;

        if (requeue) {
            // TODO: Requeue message(s) back to queue
            std.log.debug("Basic.Nack with requeue: delivery_tag={}, multiple={}", .{ delivery_tag, multiple });
        } else {
            // Reject message(s) without requeuing (discard)
            channel.ackMessage(delivery_tag, multiple);
            std.log.debug("Basic.Nack without requeue: delivery_tag={}, multiple={}", .{ delivery_tag, multiple });
        }
    }

    // Stub implementations for other basic methods
    fn handleBasicCancel(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = self;
        _ = connection;
        _ = channel_id;
        _ = payload;
        std.log.debug("Basic.Cancel not yet implemented", .{});
    }

    fn handleBasicReturn(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = self;
        _ = connection;
        _ = channel_id;
        _ = payload;
        std.log.debug("Basic.Return not yet implemented", .{});
    }

    fn handleBasicDeliver(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = self;
        _ = connection;
        _ = channel_id;
        _ = payload;
        std.log.debug("Basic.Deliver not yet implemented", .{});
    }

    fn handleBasicGet(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = self;
        _ = connection;
        _ = channel_id;
        _ = payload;
        std.log.debug("Basic.Get not yet implemented", .{});
    }

    fn handleBasicReject(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = self;
        _ = connection;
        _ = channel_id;
        _ = payload;
        std.log.debug("Basic.Reject not yet implemented", .{});
    }

    fn handleBasicRecoverAsync(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = self;
        _ = connection;
        _ = channel_id;
        _ = payload;
        std.log.debug("Basic.RecoverAsync not yet implemented", .{});
    }

    fn handleBasicRecover(self: *ProtocolHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = self;
        _ = connection;
        _ = channel_id;
        _ = payload;
        std.log.debug("Basic.Recover not yet implemented", .{});
    }

    fn handleHeaderFrame(self: *ProtocolHandler, connection: *Connection, frame: Frame) !void {
        // Check if channel exists
        const channel = connection.getChannel(frame.channel_id) orelse {
            std.log.warn("Content header frame on non-existent channel {} on connection {}", .{ frame.channel_id, connection.id });
            return error.ChannelNotFound;
        };

        if (!channel.active) {
            return error.ChannelNotActive;
        }

        // Get pending message for this channel
        const channel_key = (@as(u64, connection.id) << 16) | frame.channel_id;
        var pending_message = self.pending_messages.getPtr(channel_key) orelse {
            std.log.warn("Content header frame without preceding Basic.Publish on channel {} connection {}", .{ frame.channel_id, connection.id });
            return error.UnexpectedContentHeader;
        };

        // Parse content header
        const content_header = try ContentHeader.decode(frame.payload, self.allocator);
        
        // Validate class ID (should be 60 for Basic class)
        if (content_header.class_id != 60) {
            content_header.deinit(self.allocator);
            std.log.warn("Content header with invalid class ID {} on channel {} connection {}", .{ content_header.class_id, frame.channel_id, connection.id });
            return error.InvalidContentHeaderClass;
        }

        // Store the header and expected body size
        pending_message.header = content_header;
        pending_message.expected_body_size = content_header.body_size;

        std.log.debug("Content header received: channel={}, body_size={}, connection={}", .{ frame.channel_id, content_header.body_size, connection.id });

        // If body size is 0, complete the message immediately
        if (content_header.body_size == 0) {
            try self.completeMessage(connection, frame.channel_id);
        }
    }

    fn handleBodyFrame(self: *ProtocolHandler, connection: *Connection, frame: Frame) !void {
        _ = self;

        // TODO: Implement content body handling
        std.log.debug("Body frame not yet implemented: connection={}, channel={}", .{ connection.id, frame.channel_id });
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

    fn completeMessage(self: *ProtocolHandler, connection: *Connection, channel_id: u16) !void {
        const channel_key = (@as(u64, connection.id) << 16) | channel_id;
        
        const pending_entry = self.pending_messages.fetchRemove(channel_key) orelse {
            std.log.warn("Attempted to complete non-existent pending message on channel {} connection {}", .{ channel_id, connection.id });
            return error.NoPendingMessage;
        };
        var pending_message = pending_entry.value;
        defer pending_message.deinit(self.allocator);
        
        // Get virtual host for message routing
        const vhost = self.getVirtualHost(connection) orelse {
            std.log.warn("Cannot complete message on connection {} with no virtual host", .{connection.id});
            return error.NoVirtualHost;
        };

        // Get exchange for routing
        const exchange = vhost.getExchange(pending_message.exchange_name) orelse {
            std.log.warn("Cannot route message to non-existent exchange '{s}' on connection {}", .{ pending_message.exchange_name, connection.id });
            if (pending_message.mandatory) {
                // TODO: Send Basic.Return for mandatory message to non-existent exchange
                return error.ExchangeNotFound;
            }
            return; // Silently drop non-mandatory message
        };

        // TODO: Create complete message object and route through exchange
        // For now, just log the completion
        _ = exchange;
        
        std.log.debug("Message completed: exchange={s}, routing_key={s}, body_size={}, channel={}, connection={}", 
                     .{ pending_message.exchange_name, pending_message.routing_key, pending_message.body_data.items.len, channel_id, connection.id });
    }

    fn encodeFieldTable(self: *ProtocolHandler, table: std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage)) ![]u8 {
        var buffer = std.ArrayList(u8).init(self.allocator);
        defer buffer.deinit();

        var iterator = table.iterator();
        while (iterator.next()) |entry| {
            const key = entry.key_ptr.*;
            const value = entry.value_ptr.*;

            // Write key (short string)
            try buffer.append(@intCast(key.len));
            try buffer.appendSlice(key);

            // Write value type (long string = 'S')
            try buffer.append('S');

            // Write value length and data
            try buffer.appendSlice(&std.mem.toBytes(@as(u32, @intCast(value.len))));
            try buffer.appendSlice(value);
        }

        return buffer.toOwnedSlice();
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

test "field table encoding" {
    const allocator = std.testing.allocator;

    var handler = ProtocolHandler.init(allocator);
    defer handler.deinit();

    const encoded = try handler.encodeFieldTable(handler.server_properties);
    defer allocator.free(encoded);

    // Field table should not be empty
    try std.testing.expect(encoded.len > 0);

    // Should contain encoded key-value pairs
    // Format: key_len + key + 'S' + value_len(4 bytes) + value
    try std.testing.expect(encoded.len > 10); // At least one meaningful entry
}

test "channel method handling" {
    const allocator = std.testing.allocator;

    var handler = ProtocolHandler.init(allocator);
    defer handler.deinit();

    // Create a mock connection
    const mock_socket = std.net.Stream{ .handle = 0 };
    var connection = try Connection.init(allocator, 1, mock_socket);
    defer connection.deinit();

    // Set connection to open state for channel operations
    connection.setState(.open);

    // Test Channel.Open (method 10)
    const channel_id: u16 = 1;
    const empty_payload = [_]u8{};

    // This should succeed and create the channel
    try handler.handleChannelOpen(&connection, channel_id, &empty_payload);

    // Verify channel was created
    const channel = connection.getChannel(channel_id);
    try std.testing.expect(channel != null);
    try std.testing.expectEqual(@as(u16, 1), channel.?.id);
    try std.testing.expectEqual(true, channel.?.active);
    try std.testing.expectEqual(true, channel.?.flow_active);

    // Test Channel.Flow (method 20)
    const flow_payload = [_]u8{0}; // active = false
    try handler.handleChannelFlow(&connection, channel_id, &flow_payload);

    // Verify flow state was updated
    try std.testing.expectEqual(false, channel.?.flow_active);

    // Test Channel.Close (method 40)
    try handler.handleChannelClose(&connection, channel_id, &empty_payload);

    // Verify channel was removed
    const removed_channel = connection.getChannel(channel_id);
    try std.testing.expect(removed_channel == null);
}

test "channel method error handling" {
    const allocator = std.testing.allocator;

    var handler = ProtocolHandler.init(allocator);
    defer handler.deinit();

    const mock_socket = std.net.Stream{ .handle = 0 };
    var connection = try Connection.init(allocator, 1, mock_socket);
    defer connection.deinit();

    connection.setState(.open);

    const empty_payload = [_]u8{};

    // Test Channel.Open with invalid channel ID (0)
    const result_invalid_id = handler.handleChannelOpen(&connection, 0, &empty_payload);
    try std.testing.expectError(error.InvalidChannelId, result_invalid_id);

    // Test Channel.Flow on non-existent channel
    const flow_payload = [_]u8{1};
    const result_no_channel = handler.handleChannelFlow(&connection, 99, &flow_payload);
    try std.testing.expectError(error.ChannelNotFound, result_no_channel);

    // Test Channel.Close on non-existent channel
    const result_close_no_channel = handler.handleChannelClose(&connection, 99, &empty_payload);
    try std.testing.expectError(error.ChannelNotFound, result_close_no_channel);

    // Test Channel.Open when connection is not open
    connection.setState(.handshake);
    const result_not_open = handler.handleChannelOpen(&connection, 1, &empty_payload);
    try std.testing.expectError(error.ConnectionNotOpen, result_not_open);
}

test "exchange method handling" {
    const allocator = std.testing.allocator;

    var handler = ProtocolHandler.init(allocator);
    defer handler.deinit();

    // Create a test virtual host
    var vhost = try VirtualHost.init(allocator, "test-vhost");
    defer vhost.deinit();

    // Mock VirtualHost getter function
    const TestVHost = struct {
        var test_vhost: *VirtualHost = undefined;

        fn getVHost(vhost_name: []const u8) ?*VirtualHost {
            _ = vhost_name;
            return test_vhost;
        }
    };

    TestVHost.test_vhost = &vhost;
    handler.setVirtualHostGetter(TestVHost.getVHost);

    // Create a mock connection
    const mock_socket = std.net.Stream{ .handle = 0 };
    var connection = try Connection.init(allocator, 1, mock_socket);
    defer connection.deinit();

    connection.setState(.open);
    try connection.setVirtualHost("test-vhost");

    // Create a channel
    try connection.addChannel(1);

    // Test Exchange.Declare - create a simple payload
    var declare_payload = std.ArrayList(u8).init(allocator);
    defer declare_payload.deinit();

    // Reserved field
    try declare_payload.appendSlice(&std.mem.toBytes(@as(u16, 0)));
    // Exchange name "test-exchange"
    const exchange_name = "test-exchange";
    try declare_payload.append(@intCast(exchange_name.len));
    try declare_payload.appendSlice(exchange_name);
    // Exchange type "direct"
    const exchange_type = "direct";
    try declare_payload.append(@intCast(exchange_type.len));
    try declare_payload.appendSlice(exchange_type);
    // Flags: durable=true (0x02)
    try declare_payload.append(0x02);
    // Arguments (empty field table)
    try declare_payload.appendSlice(&std.mem.toBytes(@as(u32, 0)));

    // This should succeed and create the exchange
    try handler.handleExchangeDeclare(&connection, 1, declare_payload.items);

    // Verify exchange was created
    const exchange = vhost.getExchange("test-exchange");
    try std.testing.expect(exchange != null);
    try std.testing.expectEqual(ExchangeType.direct, exchange.?.exchange_type);
    try std.testing.expectEqual(true, exchange.?.durable);

    // Test Exchange.Delete
    var delete_payload = std.ArrayList(u8).init(allocator);
    defer delete_payload.deinit();

    // Reserved field
    try delete_payload.appendSlice(&std.mem.toBytes(@as(u16, 0)));
    // Exchange name "test-exchange"
    try delete_payload.append(@intCast(exchange_name.len));
    try delete_payload.appendSlice(exchange_name);
    // Flags: none (0x00)
    try delete_payload.append(0x00);

    // This should succeed and delete the exchange
    try handler.handleExchangeDelete(&connection, 1, delete_payload.items);

    // Verify exchange was deleted
    const deleted_exchange = vhost.getExchange("test-exchange");
    try std.testing.expect(deleted_exchange == null);
}

test "exchange method error handling" {
    const allocator = std.testing.allocator;

    var handler = ProtocolHandler.init(allocator);
    defer handler.deinit();

    const mock_socket = std.net.Stream{ .handle = 0 };
    var connection = try Connection.init(allocator, 1, mock_socket);
    defer connection.deinit();

    connection.setState(.open);

    // Test with no virtual host
    const empty_payload = [_]u8{};
    const result_no_vhost = handler.handleExchangeDeclare(&connection, 1, &empty_payload);
    try std.testing.expectError(error.NoVirtualHost, result_no_vhost);

    // Test with non-existent channel
    try connection.setVirtualHost("test-vhost");
    const result_no_channel = handler.handleExchangeDeclare(&connection, 99, &empty_payload);
    try std.testing.expectError(error.ChannelNotFound, result_no_channel);

    // Test with invalid payload
    try connection.addChannel(1);
    const result_invalid = handler.handleExchangeDeclare(&connection, 1, &empty_payload);
    try std.testing.expectError(error.InvalidExchangeDeclare, result_invalid);
}

test "queue method handling" {
    const allocator = std.testing.allocator;

    var handler = ProtocolHandler.init(allocator);
    defer handler.deinit();

    // Create a test virtual host
    var vhost = try VirtualHost.init(allocator, "test-vhost");
    defer vhost.deinit();

    // Mock VirtualHost getter function
    const TestVHost = struct {
        var test_vhost: *VirtualHost = undefined;

        fn getVHost(vhost_name: []const u8) ?*VirtualHost {
            _ = vhost_name;
            return test_vhost;
        }
    };

    TestVHost.test_vhost = &vhost;
    handler.setVirtualHostGetter(TestVHost.getVHost);

    // Create a mock connection
    const mock_socket = std.net.Stream{ .handle = 0 };
    var connection = try Connection.init(allocator, 1, mock_socket);
    defer connection.deinit();

    connection.setState(.open);
    try connection.setVirtualHost("test-vhost");

    // Create a channel
    try connection.addChannel(1);

    // Test Queue.Declare - create a simple payload
    var declare_payload = std.ArrayList(u8).init(allocator);
    defer declare_payload.deinit();

    // Reserved field
    try declare_payload.appendSlice(&std.mem.toBytes(@as(u16, 0)));
    // Queue name "test-queue"
    const queue_name = "test-queue";
    try declare_payload.append(@intCast(queue_name.len));
    try declare_payload.appendSlice(queue_name);
    // Flags: durable=true (0x02)
    try declare_payload.append(0x02);
    // Arguments (empty field table)
    try declare_payload.appendSlice(&std.mem.toBytes(@as(u32, 0)));

    // This should succeed and create the queue
    try handler.handleQueueDeclare(&connection, 1, declare_payload.items);

    // Verify queue was created
    const queue = vhost.getQueue("test-queue");
    try std.testing.expect(queue != null);
    try std.testing.expectEqual(true, queue.?.durable);

    // Test Queue.Bind - bind queue to default exchange
    var bind_payload = std.ArrayList(u8).init(allocator);
    defer bind_payload.deinit();

    // Reserved field
    try bind_payload.appendSlice(&std.mem.toBytes(@as(u16, 0)));
    // Queue name "test-queue"
    try bind_payload.append(@intCast(queue_name.len));
    try bind_payload.appendSlice(queue_name);
    // Exchange name "" (default exchange)
    const exchange_name = "";
    try bind_payload.append(@intCast(exchange_name.len));
    try bind_payload.appendSlice(exchange_name);
    // Routing key "test-key"
    const routing_key = "test-key";
    try bind_payload.append(@intCast(routing_key.len));
    try bind_payload.appendSlice(routing_key);
    // Flags: none
    try bind_payload.append(0x00);
    // Arguments (empty field table)
    try bind_payload.appendSlice(&std.mem.toBytes(@as(u32, 0)));

    // This should succeed and bind the queue
    try handler.handleQueueBind(&connection, 1, bind_payload.items);

    // Test Queue.Purge
    var purge_payload = std.ArrayList(u8).init(allocator);
    defer purge_payload.deinit();

    // Reserved field
    try purge_payload.appendSlice(&std.mem.toBytes(@as(u16, 0)));
    // Queue name "test-queue"
    try purge_payload.append(@intCast(queue_name.len));
    try purge_payload.appendSlice(queue_name);
    // Flags: none
    try purge_payload.append(0x00);

    // This should succeed and purge the queue (0 messages)
    try handler.handleQueuePurge(&connection, 1, purge_payload.items);

    // Test Queue.Unbind
    var unbind_payload = std.ArrayList(u8).init(allocator);
    defer unbind_payload.deinit();

    // Reserved field
    try unbind_payload.appendSlice(&std.mem.toBytes(@as(u16, 0)));
    // Queue name "test-queue"
    try unbind_payload.append(@intCast(queue_name.len));
    try unbind_payload.appendSlice(queue_name);
    // Exchange name "" (default exchange)
    try unbind_payload.append(@intCast(exchange_name.len));
    try unbind_payload.appendSlice(exchange_name);
    // Routing key "test-key"
    try unbind_payload.append(@intCast(routing_key.len));
    try unbind_payload.appendSlice(routing_key);
    // Arguments (empty field table)
    try unbind_payload.appendSlice(&std.mem.toBytes(@as(u32, 0)));

    // This should succeed and unbind the queue
    try handler.handleQueueUnbind(&connection, 1, unbind_payload.items);

    // Test Queue.Delete
    var delete_payload = std.ArrayList(u8).init(allocator);
    defer delete_payload.deinit();

    // Reserved field
    try delete_payload.appendSlice(&std.mem.toBytes(@as(u16, 0)));
    // Queue name "test-queue"
    try delete_payload.append(@intCast(queue_name.len));
    try delete_payload.appendSlice(queue_name);
    // Flags: none
    try delete_payload.append(0x00);

    // This should succeed and delete the queue
    try handler.handleQueueDelete(&connection, 1, delete_payload.items);

    // Verify queue was deleted
    const deleted_queue = vhost.getQueue("test-queue");
    try std.testing.expect(deleted_queue == null);
}

test "queue method error handling" {
    const allocator = std.testing.allocator;

    var handler = ProtocolHandler.init(allocator);
    defer handler.deinit();

    const mock_socket = std.net.Stream{ .handle = 0 };
    var connection = try Connection.init(allocator, 1, mock_socket);
    defer connection.deinit();

    connection.setState(.open);

    // Test with no virtual host
    const empty_payload = [_]u8{};
    const result_no_vhost = handler.handleQueueDeclare(&connection, 1, &empty_payload);
    try std.testing.expectError(error.NoVirtualHost, result_no_vhost);

    // Test with non-existent channel
    try connection.setVirtualHost("test-vhost");
    const result_no_channel = handler.handleQueueDeclare(&connection, 99, &empty_payload);
    try std.testing.expectError(error.ChannelNotFound, result_no_channel);

    // Test with invalid payload
    try connection.addChannel(1);
    const result_invalid = handler.handleQueueDeclare(&connection, 1, &empty_payload);
    try std.testing.expectError(error.InvalidQueueDeclare, result_invalid);
}
