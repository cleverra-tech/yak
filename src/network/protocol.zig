const std = @import("std");
const Connection = @import("connection.zig").Connection;
const ConnectionState = @import("connection.zig").ConnectionState;
const Frame = @import("../protocol/frame.zig").Frame;
const FrameType = @import("../protocol/frame.zig").FrameType;
const VirtualHost = @import("../core/vhost.zig").VirtualHost;
const Message = @import("../message.zig").Message;
const ErrorHelpers = @import("../error/error_handler.zig").ErrorHelpers;
const RecoveryAction = @import("../error/error_handler.zig").RecoveryAction;

const ConnectionHandler = @import("connection_handler.zig").ConnectionHandler;
const ChannelHandler = @import("channel_handler.zig").ChannelHandler;

const PendingMessage = struct {
    exchange_name: []const u8,
    routing_key: []const u8,
    body_data: std.ArrayList(u8),
    headers: ?@import("../message.zig").HeaderTable,

    pub fn deinit(self: *PendingMessage) void {
        self.body_data.deinit();
        if (self.headers) |*headers| {
            headers.deinit();
        }
    }
};

pub const ProtocolHandler = struct {
    allocator: std.mem.Allocator,
    connection_handler: ConnectionHandler,
    channel_handler: ChannelHandler,
    get_vhost_fn: ?*const fn (vhost_name: []const u8) ?*VirtualHost,
    persist_message_fn: ?*const fn (vhost_name: []const u8, queue_name: []const u8, message: *const Message) anyerror!void,
    error_handler_fn: ?*const fn (error_info: @import("../error/error_handler.zig").ErrorInfo) RecoveryAction,
    pending_messages: std.HashMap(u64, PendingMessage, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage),
    next_message_id: std.atomic.Value(u64),
    pending_messages_mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) ProtocolHandler {
        const handler = ProtocolHandler{
            .allocator = allocator,
            .connection_handler = ConnectionHandler.init(allocator) catch unreachable,
            .channel_handler = ChannelHandler.init(allocator),
            .get_vhost_fn = null,
            .persist_message_fn = null,
            .error_handler_fn = null,
            .pending_messages = std.HashMap(u64, PendingMessage, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage).init(allocator),
            .next_message_id = std.atomic.Value(u64).init(1),
            .pending_messages_mutex = std.Thread.Mutex{},
        };

        return handler;
    }

    pub fn deinit(self: *ProtocolHandler) void {
        // Clean up pending messages
        self.pending_messages_mutex.lock();
        defer self.pending_messages_mutex.unlock();

        var iterator = self.pending_messages.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.pending_messages.deinit();

        // Clean up handlers
        self.connection_handler.deinit();
    }

    pub fn setGetVirtualHostFunction(self: *ProtocolHandler, get_vhost_fn: *const fn (vhost_name: []const u8) ?*VirtualHost) void {
        self.get_vhost_fn = get_vhost_fn;
    }

    pub fn setPersistMessageFunction(self: *ProtocolHandler, persist_message_fn: *const fn (vhost_name: []const u8, queue_name: []const u8, message: *const Message) anyerror!void) void {
        self.persist_message_fn = persist_message_fn;
    }

    pub fn setErrorHandler(self: *ProtocolHandler, error_handler_fn: *const fn (error_info: @import("../error/error_handler.zig").ErrorInfo) RecoveryAction) void {
        self.error_handler_fn = error_handler_fn;
        self.connection_handler.setErrorHandler(error_handler_fn);
        self.channel_handler.setErrorHandler(error_handler_fn);
    }

    pub fn handleConnection(self: *ProtocolHandler, connection: *Connection) !void {
        // Send protocol header and start connection handshake
        try self.connection_handler.sendProtocolHeader(connection);
        try self.connection_handler.sendConnectionStart(connection);
        connection.setState(.start_sent);

        // Main frame processing loop
        while (connection.state != .closed) {
            const frame_opt = connection.receiveFrame() catch |err| {
                const error_info = ErrorHelpers.connectionError(.connection_forced, "Frame receive error", connection.id);
                if (self.error_handler_fn) |handler_fn| {
                    const action = handler_fn(error_info);
                    if (action == .close_connection) {
                        break;
                    }
                }
                return err;
            };

            if (frame_opt) |frame| {
                defer frame.deinit(self.allocator);

                self.handleFrame(connection, frame) catch |err| {
                    const error_info = ErrorHelpers.connectionError(.unexpected_frame, "Frame handling error", connection.id);
                    if (self.error_handler_fn) |handler_fn| {
                        const action = handler_fn(error_info);
                        if (action == .close_connection) {
                            break;
                        }
                    }
                    return err;
                };
            }
        }
    }

    fn handleFrame(self: *ProtocolHandler, connection: *Connection, frame: Frame) !void {
        switch (frame.frame_type) {
            .method => try self.handleMethodFrame(connection, frame),
            .header => {
                // Header frames are handled in context of pending messages
                std.log.debug("Received content header frame on channel {} for connection {}", .{ frame.channel_id, connection.id });
            },
            .body => {
                // Body frames are handled in context of pending messages
                std.log.debug("Received content body frame on channel {} for connection {}", .{ frame.channel_id, connection.id });
            },
            .heartbeat => {
                std.log.debug("Received heartbeat frame from connection {}", .{connection.id});
            },
        }
    }

    fn handleMethodFrame(self: *ProtocolHandler, connection: *Connection, frame: Frame) !void {
        if (frame.payload.len < 4) {
            return error.InvalidMethodFrame;
        }

        const class_id = std.mem.readInt(u16, frame.payload[0..2], .big);
        const method_id = std.mem.readInt(u16, frame.payload[2..4], .big);

        switch (class_id) {
            10 => try self.handleConnectionMethod(connection, method_id, frame.payload[4..]),
            20 => try self.handleChannelMethod(connection, frame.channel_id, method_id, frame.payload[4..]),
            else => {
                std.log.warn("Unknown method class {} on connection {}", .{ class_id, connection.id });
                return error.UnknownMethodClass;
            },
        }
    }

    fn handleConnectionMethod(self: *ProtocolHandler, connection: *Connection, method_id: u16, payload: []const u8) !void {
        switch (method_id) {
            11 => try self.connection_handler.handleConnectionStartOk(connection, payload), // Start-Ok
            31 => try self.connection_handler.handleConnectionTuneOk(connection, payload), // Tune-Ok
            40 => try self.connection_handler.handleConnectionOpen(connection, payload), // Open
            50 => try self.connection_handler.handleConnectionClose(connection, payload), // Close
            else => {
                std.log.warn("Unknown connection method {} on connection {}", .{ method_id, connection.id });
                return error.UnknownConnectionMethod;
            },
        }
    }

    fn handleChannelMethod(self: *ProtocolHandler, connection: *Connection, channel_id: u16, method_id: u16, payload: []const u8) !void {
        switch (method_id) {
            10 => try self.channel_handler.handleChannelOpen(connection, channel_id, payload), // Open
            20 => try self.channel_handler.handleChannelFlow(connection, channel_id, payload), // Flow
            40 => try self.channel_handler.handleChannelClose(connection, channel_id, payload), // Close
            else => {
                std.log.warn("Unknown channel method {} on channel {} connection {}", .{ method_id, channel_id, connection.id });
                return error.UnknownChannelMethod;
            },
        }
    }
};

test "protocol handler creation" {
    const allocator = std.testing.allocator;

    var handler = ProtocolHandler.init(allocator);
    defer handler.deinit();

    try std.testing.expect(handler.connection_handler.server_properties.count() > 0);
}
