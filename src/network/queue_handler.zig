const std = @import("std");
const Connection = @import("connection.zig").Connection;
const Frame = @import("../protocol/frame.zig").Frame;
const FrameType = @import("../protocol/frame.zig").FrameType;
const VirtualHost = @import("../core/vhost.zig").VirtualHost;
const Queue = @import("../routing/queue.zig").Queue;
const FieldTable = @import("field_table.zig").FieldTable;
const ErrorHelpers = @import("../error/error_handler.zig").ErrorHelpers;
const RecoveryAction = @import("../error/error_handler.zig").RecoveryAction;

/// Handles AMQP queue-level methods (Queue.Declare, Delete, Bind, Unbind, Purge)
pub const QueueHandler = struct {
    allocator: std.mem.Allocator,
    field_table: FieldTable,
    get_vhost_fn: ?*const fn (vhost_name: []const u8) ?*VirtualHost,
    error_handler_fn: ?*const fn (error_info: @import("../error/error_handler.zig").ErrorInfo) RecoveryAction,

    pub fn init(allocator: std.mem.Allocator) QueueHandler {
        return QueueHandler{
            .allocator = allocator,
            .field_table = FieldTable.init(allocator),
            .get_vhost_fn = null,
            .error_handler_fn = null,
        };
    }

    pub fn setGetVirtualHostFunction(self: *QueueHandler, get_vhost_fn: *const fn (vhost_name: []const u8) ?*VirtualHost) void {
        self.get_vhost_fn = get_vhost_fn;
    }

    pub fn setErrorHandler(self: *QueueHandler, error_handler_fn: *const fn (error_info: @import("../error/error_handler.zig").ErrorInfo) RecoveryAction) void {
        self.error_handler_fn = error_handler_fn;
    }

    pub fn handleQueueDeclare(self: *QueueHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse reserved field (2 bytes)
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Declare payload: missing reserved field", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueDeclare;
        }
        offset += 2;

        // Parse queue name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Declare payload: missing queue name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueDeclare;
        }

        const name_len = payload[offset];
        offset += 1;

        if (offset + name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Declare payload: queue name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueDeclare;
        }

        var queue_name = payload[offset .. offset + name_len];
        offset += name_len;

        // Generate unique name for anonymous queues (empty name)
        var generated_name_buf: [64]u8 = undefined;
        if (queue_name.len == 0) {
            queue_name = std.fmt.bufPrint(&generated_name_buf, "amq.gen-{}-{}", .{ connection.id, std.time.timestamp() }) catch "amq.gen-default";
        }

        // Parse flags (1 byte: passive, durable, exclusive, auto-delete, no-wait)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Declare payload: missing flags", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueDeclare;
        }

        const flags = payload[offset];
        offset += 1;

        const passive = (flags & 0x01) != 0;
        const durable = (flags & 0x02) != 0;
        const exclusive = (flags & 0x04) != 0;
        const auto_delete = (flags & 0x08) != 0;
        const no_wait = (flags & 0x10) != 0;

        // Parse arguments field table
        if (offset + 4 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Declare payload: missing arguments length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueDeclare;
        }

        const args_len = std.mem.readInt(u32, payload[offset .. offset + 4][0..4], .big);
        offset += 4;

        if (offset + args_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Declare payload: arguments data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueDeclare;
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

        var message_count: u32 = 0;
        var consumer_count: u32 = 0;

        // Handle passive mode - check if queue exists
        if (passive) {
            const existing_queue = vhost.?.getQueue(queue_name);
            if (existing_queue == null) {
                const error_info = ErrorHelpers.channelError(.not_found, "Queue does not exist", connection.id, channel_id);
                if (self.error_handler_fn) |handle_error| {
                    _ = handle_error(error_info);
                }
                return error.QueueNotFound;
            }
            message_count = existing_queue.?.getMessageCount();
            consumer_count = existing_queue.?.getConsumerCount();
        } else {
            // Create queue if it doesn't exist
            _ = vhost.?.declareQueue(queue_name, durable, exclusive, auto_delete, arguments_data) catch |err| {
                const error_info = ErrorHelpers.channelError(.resource_error, "Failed to create queue", connection.id, channel_id);
                if (self.error_handler_fn) |handle_error| {
                    _ = handle_error(error_info);
                }
                return err;
            };
            // Get the created queue for counts
            const created_queue = vhost.?.getQueue(queue_name).?;
            message_count = created_queue.getMessageCount();
            consumer_count = created_queue.getConsumerCount();
        }

        // Send Queue.DeclareOk if not no-wait
        if (!no_wait) {
            try self.sendQueueDeclareOk(connection, channel_id, queue_name, message_count, consumer_count);
        }

        std.log.debug("Queue.Declare handled for connection {}, channel {}: {s}", .{ connection.id, channel_id, queue_name });
    }

    pub fn sendQueueDeclareOk(self: *QueueHandler, connection: *Connection, channel_id: u16, queue_name: []const u8, message_count: u32, consumer_count: u32) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Queue = 50)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 50)));
        // Method ID (DeclareOk = 11)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 11)));

        // Queue name (short string)
        try payload.append(@intCast(queue_name.len));
        try payload.appendSlice(queue_name);

        // Message count
        try payload.appendSlice(&std.mem.toBytes(@as(u32, message_count)));

        // Consumer count
        try payload.appendSlice(&std.mem.toBytes(@as(u32, consumer_count)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Queue.DeclareOk sent to channel {} on connection {}: {s}", .{ channel_id, connection.id, queue_name });
    }

    pub fn handleQueueDelete(self: *QueueHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse reserved field (2 bytes)
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Delete payload: missing reserved field", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueDelete;
        }
        offset += 2;

        // Parse queue name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Delete payload: missing queue name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueDelete;
        }

        const name_len = payload[offset];
        offset += 1;

        if (offset + name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Delete payload: queue name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueDelete;
        }

        const queue_name = payload[offset .. offset + name_len];
        offset += name_len;

        // Parse flags (1 byte: if-unused, if-empty, no-wait)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Delete payload: missing flags", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueDelete;
        }

        const flags = payload[offset];
        const if_unused = (flags & 0x01) != 0;
        const if_empty = (flags & 0x02) != 0;
        const no_wait = (flags & 0x04) != 0;

        // Get virtual host
        const vhost = if (self.get_vhost_fn) |get_vhost| get_vhost(connection.virtual_host orelse "/") else null;
        if (vhost == null) {
            const error_info = ErrorHelpers.channelError(.not_found, "Virtual host not found", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.VirtualHostNotFound;
        }

        // Delete queue
        const message_count = vhost.?.deleteQueue(queue_name, if_unused, if_empty) catch |err| {
            const error_info = ErrorHelpers.channelError(.precondition_failed, "Failed to delete queue", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return err;
        };

        // Send Queue.DeleteOk if not no-wait
        if (!no_wait) {
            try self.sendQueueDeleteOk(connection, channel_id, message_count);
        }

        std.log.debug("Queue.Delete handled for connection {}, channel {}: {s}", .{ connection.id, channel_id, queue_name });
    }

    pub fn sendQueueDeleteOk(self: *QueueHandler, connection: *Connection, channel_id: u16, message_count: u32) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Queue = 50)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 50)));
        // Method ID (DeleteOk = 21)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 21)));

        // Message count
        try payload.appendSlice(&std.mem.toBytes(@as(u32, message_count)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Queue.DeleteOk sent to channel {} on connection {}", .{ channel_id, connection.id });
    }

    pub fn handleQueueBind(self: *QueueHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse reserved field (2 bytes)
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Bind payload: missing reserved field", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueBind;
        }
        offset += 2;

        // Parse queue name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Bind payload: missing queue name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueBind;
        }

        const queue_name_len = payload[offset];
        offset += 1;

        if (offset + queue_name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Bind payload: queue name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueBind;
        }

        const queue_name = payload[offset .. offset + queue_name_len];
        offset += queue_name_len;

        // Parse exchange name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Bind payload: missing exchange name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueBind;
        }

        const exchange_name_len = payload[offset];
        offset += 1;

        if (offset + exchange_name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Bind payload: exchange name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueBind;
        }

        const exchange_name = payload[offset .. offset + exchange_name_len];
        offset += exchange_name_len;

        // Parse routing key (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Bind payload: missing routing key length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueBind;
        }

        const routing_key_len = payload[offset];
        offset += 1;

        if (offset + routing_key_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Bind payload: routing key data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueBind;
        }

        const routing_key = payload[offset .. offset + routing_key_len];
        offset += routing_key_len;

        // Parse no-wait flag (1 byte)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Bind payload: missing no-wait flag", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueBind;
        }

        const no_wait = payload[offset] != 0;
        offset += 1;

        // Parse arguments field table
        if (offset + 4 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Bind payload: missing arguments length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueBind;
        }

        const args_len = std.mem.readInt(u32, payload[offset .. offset + 4][0..4], .big);
        offset += 4;

        if (offset + args_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Bind payload: arguments data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueBind;
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

        // Bind queue to exchange
        vhost.?.bindQueue(queue_name, exchange_name, routing_key, arguments_data) catch |err| {
            const error_info = ErrorHelpers.channelError(.not_found, "Failed to bind queue to exchange", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return err;
        };

        // Send Queue.BindOk if not no-wait
        if (!no_wait) {
            try self.sendQueueBindOk(connection, channel_id);
        }

        std.log.debug("Queue.Bind handled for connection {}, channel {}: {s} -> {s}", .{ connection.id, channel_id, exchange_name, queue_name });
    }

    pub fn sendQueueBindOk(self: *QueueHandler, connection: *Connection, channel_id: u16) !void {
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
        std.log.debug("Queue.BindOk sent to channel {} on connection {}", .{ channel_id, connection.id });
    }

    pub fn handleQueueUnbind(self: *QueueHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse reserved field (2 bytes)
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Unbind payload: missing reserved field", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueUnbind;
        }
        offset += 2;

        // Parse queue name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Unbind payload: missing queue name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueUnbind;
        }

        const queue_name_len = payload[offset];
        offset += 1;

        if (offset + queue_name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Unbind payload: queue name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueUnbind;
        }

        const queue_name = payload[offset .. offset + queue_name_len];
        offset += queue_name_len;

        // Parse exchange name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Unbind payload: missing exchange name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueUnbind;
        }

        const exchange_name_len = payload[offset];
        offset += 1;

        if (offset + exchange_name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Unbind payload: exchange name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueUnbind;
        }

        const exchange_name = payload[offset .. offset + exchange_name_len];
        offset += exchange_name_len;

        // Parse routing key (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Unbind payload: missing routing key length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueUnbind;
        }

        const routing_key_len = payload[offset];
        offset += 1;

        if (offset + routing_key_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Unbind payload: routing key data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueUnbind;
        }

        const routing_key = payload[offset .. offset + routing_key_len];
        offset += routing_key_len;

        // Parse arguments field table
        if (offset + 4 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Unbind payload: missing arguments length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueUnbind;
        }

        const args_len = std.mem.readInt(u32, payload[offset .. offset + 4][0..4], .big);
        offset += 4;

        if (offset + args_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Unbind payload: arguments data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueueUnbind;
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

        // Unbind queue from exchange
        vhost.?.unbindQueue(queue_name, exchange_name, routing_key, arguments_data) catch |err| {
            const error_info = ErrorHelpers.channelError(.not_found, "Failed to unbind queue from exchange", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return err;
        };

        // Send Queue.UnbindOk
        try self.sendQueueUnbindOk(connection, channel_id);

        std.log.debug("Queue.Unbind handled for connection {}, channel {}: {s} -/-> {s}", .{ connection.id, channel_id, exchange_name, queue_name });
    }

    pub fn sendQueueUnbindOk(self: *QueueHandler, connection: *Connection, channel_id: u16) !void {
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
        std.log.debug("Queue.UnbindOk sent to channel {} on connection {}", .{ channel_id, connection.id });
    }

    pub fn handleQueuePurge(self: *QueueHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse reserved field (2 bytes)
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Purge payload: missing reserved field", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueuePurge;
        }
        offset += 2;

        // Parse queue name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Purge payload: missing queue name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueuePurge;
        }

        const name_len = payload[offset];
        offset += 1;

        if (offset + name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Purge payload: queue name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueuePurge;
        }

        const queue_name = payload[offset .. offset + name_len];
        offset += name_len;

        // Parse no-wait flag (1 byte)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Queue.Purge payload: missing no-wait flag", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidQueuePurge;
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

        // Purge queue
        const message_count = vhost.?.purgeQueue(queue_name) catch |err| {
            const error_info = ErrorHelpers.channelError(.not_found, "Failed to purge queue", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return err;
        };

        // Send Queue.PurgeOk if not no-wait
        if (!no_wait) {
            try self.sendQueuePurgeOk(connection, channel_id, message_count);
        }

        std.log.debug("Queue.Purge handled for connection {}, channel {}: {s} ({} messages)", .{ connection.id, channel_id, queue_name, message_count });
    }

    pub fn sendQueuePurgeOk(self: *QueueHandler, connection: *Connection, channel_id: u16, message_count: u32) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Queue = 50)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 50)));
        // Method ID (PurgeOk = 31)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 31)));

        // Message count
        try payload.appendSlice(&std.mem.toBytes(@as(u32, message_count)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Queue.PurgeOk sent to channel {} on connection {}", .{ channel_id, connection.id });
    }
};

test "queue handler creation" {
    const allocator = std.testing.allocator;

    const handler = QueueHandler.init(allocator);
    try std.testing.expect(handler.get_vhost_fn == null);
    try std.testing.expect(handler.error_handler_fn == null);
}
