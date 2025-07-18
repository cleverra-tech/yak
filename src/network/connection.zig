const std = @import("std");
const Frame = @import("../protocol/frame.zig").Frame;
const FrameType = @import("../protocol/frame.zig").FrameType;
const Method = @import("../protocol/methods.zig").Method;

pub const ConnectionState = enum {
    handshake,
    start_sent,
    start_ok_received,
    tune_sent,
    tune_ok_received,
    open_received,
    open_ok_sent,
    open,
    closing,
    closed,
};

pub const Connection = struct {
    id: u64,
    socket: std.net.Stream,
    state: ConnectionState,
    authenticated: bool,
    virtual_host: ?[]const u8,
    channels: std.HashMap(u16, *Channel, std.hash_map.AutoContext(u16), std.hash_map.default_max_load_percentage),

    // Connection properties
    heartbeat_interval: u16,
    last_heartbeat: i64,
    max_frame_size: u32,
    channel_max: u16,

    // Flow control
    blocked: bool,

    // Buffer management
    read_buffer: []u8,
    write_buffer: std.ArrayList(u8),

    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,

    const Channel = struct {
        id: u16,
        active: bool,
        flow_active: bool,
        connection_id: u64,
        prefetch_count: u16,
        prefetch_size: u32,
        unacked_messages: std.ArrayList(u64),
        allocator: std.mem.Allocator,

        pub fn init(allocator: std.mem.Allocator, id: u16, connection_id: u64) Channel {
            return Channel{
                .id = id,
                .active = true,
                .flow_active = true,
                .connection_id = connection_id,
                .prefetch_count = 0,
                .prefetch_size = 0,
                .unacked_messages = std.ArrayList(u64).init(allocator),
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Channel) void {
            self.unacked_messages.deinit();
        }

        pub fn canReceiveMore(self: *const Channel) bool {
            if (self.prefetch_count > 0) {
                return self.unacked_messages.items.len < self.prefetch_count;
            }
            return true;
        }

        pub fn addUnackedMessage(self: *Channel, delivery_tag: u64) !void {
            try self.unacked_messages.append(delivery_tag);
        }

        pub fn ackMessage(self: *Channel, delivery_tag: u64, multiple: bool) void {
            if (multiple) {
                var i: usize = 0;
                while (i < self.unacked_messages.items.len) {
                    if (self.unacked_messages.items[i] <= delivery_tag) {
                        _ = self.unacked_messages.swapRemove(i);
                    } else {
                        i += 1;
                    }
                }
            } else {
                for (self.unacked_messages.items, 0..) |tag, i| {
                    if (tag == delivery_tag) {
                        _ = self.unacked_messages.swapRemove(i);
                        break;
                    }
                }
            }
        }
    };

    pub fn init(allocator: std.mem.Allocator, id: u64, socket: std.net.Stream) !Connection {
        const read_buffer = try allocator.alloc(u8, 65536); // 64KB read buffer

        return Connection{
            .id = id,
            .socket = socket,
            .state = .handshake,
            .authenticated = false,
            .virtual_host = null,
            .channels = std.HashMap(u16, *Channel, std.hash_map.AutoContext(u16), std.hash_map.default_max_load_percentage).init(allocator),
            .heartbeat_interval = 0,
            .last_heartbeat = std.time.timestamp(),
            .max_frame_size = 131072, // 128KB default
            .channel_max = 1000,
            .blocked = false,
            .read_buffer = read_buffer,
            .write_buffer = std.ArrayList(u8).init(allocator),
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Connection) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Clean up channels
        var iterator = self.channels.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.channels.deinit();

        // Clean up buffers
        self.allocator.free(self.read_buffer);
        self.write_buffer.deinit();

        // Close socket
        self.socket.close();

        // Clean up virtual host name
        if (self.virtual_host) |vhost| {
            self.allocator.free(vhost);
        }
    }

    pub fn addChannel(self: *Connection, channel_id: u16) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.channels.contains(channel_id)) {
            return error.ChannelAlreadyExists;
        }

        const channel = try self.allocator.create(Channel);
        channel.* = Channel.init(self.allocator, channel_id, self.id);
        try self.channels.put(channel_id, channel);

        std.log.debug("Channel {} created for connection {}", .{ channel_id, self.id });
    }

    pub fn removeChannel(self: *Connection, channel_id: u16) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.channels.fetchRemove(channel_id)) |entry| {
            entry.value.deinit();
            self.allocator.destroy(entry.value);
            std.log.debug("Channel {} removed from connection {}", .{ channel_id, self.id });
        }
    }

    pub fn getChannel(self: *Connection, channel_id: u16) ?*Channel {
        return self.channels.get(channel_id);
    }

    pub fn setState(self: *Connection, new_state: ConnectionState) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        std.log.debug("Connection {} state change: {} -> {}", .{ self.id, self.state, new_state });
        self.state = new_state;
    }

    pub fn isOpen(self: *const Connection) bool {
        return self.state == .open;
    }

    pub fn isClosing(self: *const Connection) bool {
        return self.state == .closing or self.state == .closed;
    }

    pub fn sendFrame(self: *Connection, frame: Frame) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Encode frame to write buffer
        try frame.encode(&self.write_buffer);

        // Send data
        const bytes_written = try self.socket.write(self.write_buffer.items);
        if (bytes_written != self.write_buffer.items.len) {
            return error.PartialWrite;
        }

        // Clear write buffer for next frame
        self.write_buffer.clearRetainingCapacity();

        std.log.debug("Frame sent on connection {}: type={}, channel={}, size={}", .{ self.id, frame.frame_type, frame.channel, bytes_written });
    }

    pub fn receiveFrame(self: *Connection) !?Frame {
        // Read frame header first (8 bytes: type + channel + size + end)
        var header_buf: [8]u8 = undefined;
        const header_bytes = self.socket.read(&header_buf) catch |err| {
            std.log.debug("Connection {} read error: {}", .{ self.id, err });
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

        if (payload_size > self.max_frame_size) {
            return error.FrameTooLarge;
        }

        // Read payload
        var payload = try self.allocator.alloc(u8, payload_size);
        defer self.allocator.free(payload);

        const payload_bytes = try self.socket.readAll(payload);
        if (payload_bytes != payload_size) {
            return error.IncompletePayload;
        }

        // Create and return frame
        const frame = Frame{
            .frame_type = frame_type,
            .channel = channel,
            .payload = try self.allocator.dupe(u8, payload),
        };

        std.log.debug("Frame received on connection {}: type={}, channel={}, size={}", .{ self.id, frame_type, channel, payload_size });

        return frame;
    }

    pub fn sendHeartbeat(self: *Connection) !void {
        const heartbeat_frame = Frame{
            .frame_type = .heartbeat,
            .channel = 0,
            .payload = &[_]u8{},
        };

        try self.sendFrame(heartbeat_frame);
        self.last_heartbeat = std.time.timestamp();

        std.log.debug("Heartbeat sent on connection {}", .{self.id});
    }

    pub fn checkHeartbeat(self: *Connection) bool {
        if (self.heartbeat_interval == 0) {
            return true; // Heartbeat disabled
        }

        const now = std.time.timestamp();
        const elapsed = now - self.last_heartbeat;
        const timeout = @as(i64, self.heartbeat_interval) * 2; // 2x heartbeat interval

        if (elapsed > timeout) {
            std.log.warn("Connection {} heartbeat timeout: {}s > {}s", .{ self.id, elapsed, timeout });
            return false;
        }

        return true;
    }

    pub fn setVirtualHost(self: *Connection, vhost_name: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.virtual_host) |old_vhost| {
            self.allocator.free(old_vhost);
        }

        self.virtual_host = try self.allocator.dupe(u8, vhost_name);
        std.log.debug("Connection {} virtual host set to: {s}", .{ self.id, vhost_name });
    }

    pub fn getStats(self: *const Connection, allocator: std.mem.Allocator) !std.json.Value {
        var stats = std.json.ObjectMap.init(allocator);

        try stats.put("id", std.json.Value{ .integer = @intCast(self.id) });
        try stats.put("state", std.json.Value{ .string = @tagName(self.state) });
        try stats.put("authenticated", std.json.Value{ .bool = self.authenticated });
        try stats.put("channels", std.json.Value{ .integer = @intCast(self.channels.count()) });
        try stats.put("heartbeat_interval", std.json.Value{ .integer = self.heartbeat_interval });
        try stats.put("max_frame_size", std.json.Value{ .integer = @intCast(self.max_frame_size) });
        try stats.put("channel_max", std.json.Value{ .integer = self.channel_max });
        try stats.put("blocked", std.json.Value{ .bool = self.blocked });

        if (self.virtual_host) |vhost| {
            try stats.put("virtual_host", std.json.Value{ .string = vhost });
        }

        return std.json.Value{ .object = stats };
    }
};

test "connection creation and basic operations" {
    const allocator = std.testing.allocator;

    // Create a mock socket (would need actual implementation for real tests)
    const mock_socket = std.net.Stream{ .handle = 0 };

    var connection = try Connection.init(allocator, 1, mock_socket);
    defer connection.deinit();

    try std.testing.expectEqual(@as(u64, 1), connection.id);
    try std.testing.expectEqual(ConnectionState.handshake, connection.state);
    try std.testing.expectEqual(false, connection.authenticated);
    try std.testing.expectEqual(@as(u32, 0), connection.channels.count());

    // Test channel management
    try connection.addChannel(1);
    try std.testing.expectEqual(@as(u32, 1), connection.channels.count());

    const channel = connection.getChannel(1);
    try std.testing.expect(channel != null);
    try std.testing.expectEqual(@as(u16, 1), channel.?.id);

    connection.removeChannel(1);
    try std.testing.expectEqual(@as(u32, 0), connection.channels.count());
}

test "connection state management" {
    const allocator = std.testing.allocator;
    const mock_socket = std.net.Stream{ .handle = 0 };

    var connection = try Connection.init(allocator, 1, mock_socket);
    defer connection.deinit();

    try std.testing.expectEqual(false, connection.isOpen());
    try std.testing.expectEqual(false, connection.isClosing());

    connection.setState(.open);
    try std.testing.expectEqual(true, connection.isOpen());
    try std.testing.expectEqual(false, connection.isClosing());

    connection.setState(.closing);
    try std.testing.expectEqual(false, connection.isOpen());
    try std.testing.expectEqual(true, connection.isClosing());
}
