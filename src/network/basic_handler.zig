const std = @import("std");
const Connection = @import("connection.zig").Connection;
const Frame = @import("../protocol/frame.zig").Frame;
const FrameType = @import("../protocol/frame.zig").FrameType;
const VirtualHost = @import("../core/vhost.zig").VirtualHost;
const Message = @import("../message.zig").Message;
const FieldTable = @import("field_table.zig").FieldTable;
const ErrorHelpers = @import("../error/error_handler.zig").ErrorHelpers;
const RecoveryAction = @import("../error/error_handler.zig").RecoveryAction;

/// Handles AMQP basic methods (Basic.Publish, Consume, Get, Ack, Nack, Reject, Recover)
pub const BasicHandler = struct {
    allocator: std.mem.Allocator,
    field_table: FieldTable,
    get_vhost_fn: ?*const fn (vhost_name: []const u8) ?*VirtualHost,
    error_handler_fn: ?*const fn (error_info: @import("../error/error_handler.zig").ErrorInfo) RecoveryAction,

    pub fn init(allocator: std.mem.Allocator) BasicHandler {
        return BasicHandler{
            .allocator = allocator,
            .field_table = FieldTable.init(allocator),
            .get_vhost_fn = null,
            .error_handler_fn = null,
        };
    }

    pub fn setGetVirtualHostFunction(self: *BasicHandler, get_vhost_fn: *const fn (vhost_name: []const u8) ?*VirtualHost) void {
        self.get_vhost_fn = get_vhost_fn;
    }

    pub fn setErrorHandler(self: *BasicHandler, error_handler_fn: *const fn (error_info: @import("../error/error_handler.zig").ErrorInfo) RecoveryAction) void {
        self.error_handler_fn = error_handler_fn;
    }

    pub fn handleBasicPublish(self: *BasicHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse reserved field (2 bytes)
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Publish payload: missing reserved field", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicPublish;
        }
        offset += 2;

        // Parse exchange name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Publish payload: missing exchange name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicPublish;
        }

        const exchange_name_len = payload[offset];
        offset += 1;

        if (offset + exchange_name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Publish payload: exchange name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicPublish;
        }

        const exchange_name = payload[offset .. offset + exchange_name_len];
        offset += exchange_name_len;

        // Parse routing key (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Publish payload: missing routing key length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicPublish;
        }

        const routing_key_len = payload[offset];
        offset += 1;

        if (offset + routing_key_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Publish payload: routing key data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicPublish;
        }

        const routing_key = payload[offset .. offset + routing_key_len];
        offset += routing_key_len;

        // Parse flags (1 byte: mandatory, immediate)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Publish payload: missing flags", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicPublish;
        }

        const flags = payload[offset];
        const mandatory = (flags & 0x01) != 0;
        const immediate = (flags & 0x02) != 0;

        // Get virtual host
        const vhost = if (self.get_vhost_fn) |get_vhost| get_vhost(connection.virtual_host orelse "/") else null;
        if (vhost == null) {
            const error_info = ErrorHelpers.channelError(.not_found, "Virtual host not found", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.VirtualHostNotFound;
        }

        // Basic.Publish requires header and body frames to follow - stub implementation
        _ = mandatory;
        _ = immediate;

        // TODO: For now, just acknowledge the publish command without implementing full message routing
        // This would need content header and body frame handling

        std.log.debug("Basic.Publish handled for connection {}, channel {}: {s} -> {s}", .{ connection.id, channel_id, exchange_name, routing_key });
    }

    pub fn handleBasicConsume(self: *BasicHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse reserved field (2 bytes)
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Consume payload: missing reserved field", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicConsume;
        }
        offset += 2;

        // Parse queue name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Consume payload: missing queue name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicConsume;
        }

        const queue_name_len = payload[offset];
        offset += 1;

        if (offset + queue_name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Consume payload: queue name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicConsume;
        }

        const queue_name = payload[offset .. offset + queue_name_len];
        offset += queue_name_len;

        // Parse consumer tag (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Consume payload: missing consumer tag length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicConsume;
        }

        const consumer_tag_len = payload[offset];
        offset += 1;

        if (offset + consumer_tag_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Consume payload: consumer tag data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicConsume;
        }

        var consumer_tag = payload[offset .. offset + consumer_tag_len];
        offset += consumer_tag_len;

        // Generate unique consumer tag if empty
        var generated_tag_buf: [64]u8 = undefined;
        if (consumer_tag.len == 0) {
            consumer_tag = std.fmt.bufPrint(&generated_tag_buf, "ctag-{}-{}", .{ connection.id, std.time.timestamp() }) catch "ctag-default";
        }

        // Parse flags (1 byte: no-local, no-ack, exclusive, no-wait)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Consume payload: missing flags", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicConsume;
        }

        const flags = payload[offset];
        offset += 1;

        const no_local = (flags & 0x01) != 0;
        const no_ack = (flags & 0x02) != 0;
        const exclusive = (flags & 0x04) != 0;
        const no_wait = (flags & 0x08) != 0;

        // Parse arguments field table
        if (offset + 4 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Consume payload: missing arguments length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicConsume;
        }

        const args_len = std.mem.readInt(u32, payload[offset .. offset + 4][0..4], .big);
        offset += 4;

        if (offset + args_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Consume payload: arguments data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicConsume;
        }

        const arguments_data = if (args_len > 0) payload[offset .. offset + args_len] else null;

        // Get virtual host
        const vhost = if (self.get_vhost_fn) |get_vhost| get_vhost(connection.virtual_host orelse "/") else null;
        if (vhost == null) {
            const error_info = ErrorHelpers.channelError(.not_found, "Virtual host not found", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.VirtualHostNotFound;
        }

        // Start consuming - stub implementation
        _ = no_local;
        _ = no_ack;
        _ = exclusive;
        _ = arguments_data;

        // TODO: For now, just acknowledge the consume command
        // Full consumer implementation would require message delivery

        // Send Basic.ConsumeOk if not no-wait
        if (!no_wait) {
            try self.sendBasicConsumeOk(connection, channel_id, consumer_tag);
        }

        std.log.debug("Basic.Consume handled for connection {}, channel {}: {s} (tag: {s})", .{ connection.id, channel_id, queue_name, consumer_tag });
    }

    pub fn sendBasicConsumeOk(self: *BasicHandler, connection: *Connection, channel_id: u16, consumer_tag: []const u8) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Basic = 60)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 60)));
        // Method ID (ConsumeOk = 21)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 21)));

        // Consumer tag (short string)
        try payload.append(@intCast(consumer_tag.len));
        try payload.appendSlice(consumer_tag);

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Basic.ConsumeOk sent to channel {} on connection {}: {s}", .{ channel_id, connection.id, consumer_tag });
    }

    pub fn handleBasicCancel(self: *BasicHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse consumer tag (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Cancel payload: missing consumer tag length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicCancel;
        }

        const consumer_tag_len = payload[offset];
        offset += 1;

        if (offset + consumer_tag_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Cancel payload: consumer tag data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicCancel;
        }

        const consumer_tag = payload[offset .. offset + consumer_tag_len];
        offset += consumer_tag_len;

        // Parse no-wait flag (1 byte)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Cancel payload: missing no-wait flag", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicCancel;
        }

        const no_wait = payload[offset] != 0;

        // Get virtual host
        const vhost = if (self.get_vhost_fn) |get_vhost| get_vhost(connection.virtual_host orelse "/") else null;
        if (vhost == null) {
            const error_info = ErrorHelpers.channelError(.not_found, "Virtual host not found", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.VirtualHostNotFound;
        }

        // Cancel consumer - stub implementation

        // TODO: For now, just acknowledge the cancel command

        // Send Basic.CancelOk if not no-wait
        if (!no_wait) {
            try self.sendBasicCancelOk(connection, channel_id, consumer_tag);
        }

        std.log.debug("Basic.Cancel handled for connection {}, channel {}: {s}", .{ connection.id, channel_id, consumer_tag });
    }

    pub fn sendBasicCancelOk(self: *BasicHandler, connection: *Connection, channel_id: u16, consumer_tag: []const u8) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Basic = 60)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 60)));
        // Method ID (CancelOk = 31)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 31)));

        // Consumer tag (short string)
        try payload.append(@intCast(consumer_tag.len));
        try payload.appendSlice(consumer_tag);

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Basic.CancelOk sent to channel {} on connection {}: {s}", .{ channel_id, connection.id, consumer_tag });
    }

    pub fn handleBasicGet(self: *BasicHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse reserved field (2 bytes)
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Get payload: missing reserved field", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicGet;
        }
        offset += 2;

        // Parse queue name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Get payload: missing queue name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicGet;
        }

        const queue_name_len = payload[offset];
        offset += 1;

        if (offset + queue_name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Get payload: queue name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicGet;
        }

        const queue_name = payload[offset .. offset + queue_name_len];
        offset += queue_name_len;

        // Parse no-ack flag (1 byte)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Get payload: missing no-ack flag", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicGet;
        }

        const no_ack = payload[offset] != 0;

        // Get virtual host
        const vhost = if (self.get_vhost_fn) |get_vhost| get_vhost(connection.virtual_host orelse "/") else null;
        if (vhost == null) {
            const error_info = ErrorHelpers.channelError(.not_found, "Virtual host not found", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.VirtualHostNotFound;
        }

        // Get message from queue - stub implementation
        _ = no_ack;

        // TODO: For now, always return empty (no messages available)
        try self.sendBasicGetEmpty(connection, channel_id);

        std.log.debug("Basic.Get handled for connection {}, channel {}: {s}", .{ connection.id, channel_id, queue_name });
    }

    pub fn sendBasicGetOk(self: *BasicHandler, connection: *Connection, channel_id: u16, delivery_tag: u64, redelivered: bool, exchange_name: []const u8, routing_key: []const u8, message_count: u32) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Basic = 60)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 60)));
        // Method ID (GetOk = 71)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 71)));

        // Delivery tag
        try payload.appendSlice(&std.mem.toBytes(@as(u64, delivery_tag)));

        // Redelivered flag
        try payload.append(if (redelivered) 1 else 0);

        // Exchange name (short string)
        try payload.append(@intCast(exchange_name.len));
        try payload.appendSlice(exchange_name);

        // Routing key (short string)
        try payload.append(@intCast(routing_key.len));
        try payload.appendSlice(routing_key);

        // Message count
        try payload.appendSlice(&std.mem.toBytes(@as(u32, message_count)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Basic.GetOk sent to channel {} on connection {}", .{ channel_id, connection.id });
    }

    pub fn sendBasicGetEmpty(self: *BasicHandler, connection: *Connection, channel_id: u16) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Basic = 60)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 60)));
        // Method ID (GetEmpty = 72)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 72)));

        // Cluster ID (short string) - empty
        try payload.append(0);

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Basic.GetEmpty sent to channel {} on connection {}", .{ channel_id, connection.id });
    }

    pub fn handleBasicAck(self: *BasicHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        if (payload.len < 9) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Ack payload: insufficient data", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicAck;
        }

        const delivery_tag = std.mem.readInt(u64, payload[0..8], .big);
        const multiple = payload[8] != 0;

        // Get virtual host
        const vhost = if (self.get_vhost_fn) |get_vhost| get_vhost(connection.virtual_host orelse "/") else null;
        if (vhost == null) {
            const error_info = ErrorHelpers.channelError(.not_found, "Virtual host not found", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.VirtualHostNotFound;
        }

        // Acknowledge message(s) - stub implementation

        // TODO: For now, just acknowledge the ack command

        std.log.debug("Basic.Ack handled for connection {}, channel {}: delivery_tag={}, multiple={}", .{ connection.id, channel_id, delivery_tag, multiple });
    }

    pub fn handleBasicNack(self: *BasicHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        if (payload.len < 10) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Nack payload: insufficient data", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicNack;
        }

        const delivery_tag = std.mem.readInt(u64, payload[0..8], .big);
        const multiple = payload[8] != 0;
        const requeue = payload[9] != 0;

        // Get virtual host
        const vhost = if (self.get_vhost_fn) |get_vhost| get_vhost(connection.virtual_host orelse "/") else null;
        if (vhost == null) {
            const error_info = ErrorHelpers.channelError(.not_found, "Virtual host not found", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.VirtualHostNotFound;
        }

        // Negative acknowledge message(s) with dead letter support
        // TODO: In a full implementation, we would need to track which queue(s)
        // the message(s) came from based on the delivery tag(s) and channel state
        // For now, we'll log the nack

        std.log.debug("Basic.Nack handled for connection {}, channel {}: delivery_tag={}, multiple={}, requeue={}", .{ connection.id, channel_id, delivery_tag, multiple, requeue });
    }

    pub fn handleBasicReject(self: *BasicHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        if (payload.len < 9) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Reject payload: insufficient data", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicReject;
        }

        const delivery_tag = std.mem.readInt(u64, payload[0..8], .big);
        const requeue = payload[8] != 0;

        // Get virtual host
        const vhost = if (self.get_vhost_fn) |get_vhost| get_vhost(connection.virtual_host orelse "/") else null;
        if (vhost == null) {
            const error_info = ErrorHelpers.channelError(.not_found, "Virtual host not found", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.VirtualHostNotFound;
        }

        // Reject message with dead letter support
        // TODO: In a full implementation, we would need to track which queue
        // the message came from based on the delivery tag and channel state
        // For now, we'll log the rejection

        std.log.debug("Basic.Reject handled for connection {}, channel {}: delivery_tag={}, requeue={}", .{ connection.id, channel_id, delivery_tag, requeue });
    }

    pub fn handleBasicRecover(self: *BasicHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        if (payload.len < 1) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Basic.Recover payload: missing requeue flag", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidBasicRecover;
        }

        const requeue = payload[0] != 0;

        // Get virtual host
        const vhost = if (self.get_vhost_fn) |get_vhost| get_vhost(connection.virtual_host orelse "/") else null;
        if (vhost == null) {
            const error_info = ErrorHelpers.channelError(.not_found, "Virtual host not found", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.VirtualHostNotFound;
        }

        // Recover unacknowledged messages - stub implementation

        // TODO: For now, just acknowledge the recover command

        // Send Basic.RecoverOk
        try self.sendBasicRecoverOk(connection, channel_id);

        std.log.debug("Basic.Recover handled for connection {}, channel {}: requeue={}", .{ connection.id, channel_id, requeue });
    }

    pub fn sendBasicRecoverOk(self: *BasicHandler, connection: *Connection, channel_id: u16) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Basic = 60)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 60)));
        // Method ID (RecoverOk = 111)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 111)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Basic.RecoverOk sent to channel {} on connection {}", .{ channel_id, connection.id });
    }
};

test "basic handler creation" {
    const allocator = std.testing.allocator;

    const handler = BasicHandler.init(allocator);
    try std.testing.expect(handler.get_vhost_fn == null);
    try std.testing.expect(handler.error_handler_fn == null);
}
