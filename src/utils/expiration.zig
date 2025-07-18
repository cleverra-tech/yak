const std = @import("std");

/// Time-based expiration utility for messages and other timed resources
pub const ExpirationManager = struct {
    entries: std.HashMap(u64, ExpirationEntry, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage),
    heap: std.PriorityQueue(ExpirationEntry, void, compareEntries),
    next_id: u64,
    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,

    const ExpirationEntry = struct {
        id: u64,
        expires_at: i64,
        callback: *const fn (id: u64, data: ?*anyopaque) void,
        data: ?*anyopaque,
    };

    pub fn init(allocator: std.mem.Allocator) ExpirationManager {
        return ExpirationManager{
            .entries = std.HashMap(u64, ExpirationEntry, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage).init(allocator),
            .heap = std.PriorityQueue(ExpirationEntry, void, compareEntries).init(allocator, {}),
            .next_id = 1,
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *ExpirationManager) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.entries.deinit();
        self.heap.deinit();
    }

    pub fn add(self: *ExpirationManager, ttl_seconds: u32, callback: *const fn (id: u64, data: ?*anyopaque) void, data: ?*anyopaque) !u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const id = self.next_id;
        self.next_id += 1;

        const expires_at = std.time.timestamp() + @as(i64, ttl_seconds);
        const entry = ExpirationEntry{
            .id = id,
            .expires_at = expires_at,
            .callback = callback,
            .data = data,
        };

        try self.entries.put(id, entry);
        try self.heap.add(entry);

        return id;
    }

    pub fn remove(self: *ExpirationManager, id: u64) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.entries.fetchRemove(id)) |_| {
            // Note: We don't remove from heap for efficiency - it will be skipped during processing
            return true;
        }
        return false;
    }

    pub fn processExpired(self: *ExpirationManager) u32 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = std.time.timestamp();
        var expired_count: u32 = 0;

        while (self.heap.peek()) |top_entry| {
            if (top_entry.expires_at > now) {
                break; // No more expired entries
            }

            const entry = self.heap.remove();

            // Check if entry is still valid (not removed)
            if (self.entries.fetchRemove(entry.id)) |removed_entry| {
                entry.callback(entry.id, entry.data);
                expired_count += 1;
            }
        }

        return expired_count;
    }

    pub fn getNextExpirationTime(self: *ExpirationManager) ?i64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.heap.peek()) |top_entry| {
            // Check if this entry is still valid
            if (self.entries.contains(top_entry.id)) {
                return top_entry.expires_at;
            } else {
                // Remove invalid entry from heap
                _ = self.heap.remove();
            }
        }

        return null;
    }

    pub fn getCount(self: *ExpirationManager) usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.entries.count();
    }

    fn compareEntries(context: void, a: ExpirationEntry, b: ExpirationEntry) std.math.Order {
        _ = context;
        return std.math.order(a.expires_at, b.expires_at);
    }
};

/// Message TTL (Time To Live) manager for AMQP messages
pub const MessageTTL = struct {
    expiration_manager: ExpirationManager,
    message_callbacks: std.HashMap(u64, MessageCallback, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage),
    allocator: std.mem.Allocator,

    const MessageCallback = struct {
        queue_name: []const u8,
        message_id: u64,
        expiry_callback: *const fn (queue_name: []const u8, message_id: u64) void,
    };

    pub fn init(allocator: std.mem.Allocator) MessageTTL {
        return MessageTTL{
            .expiration_manager = ExpirationManager.init(allocator),
            .message_callbacks = std.HashMap(u64, MessageCallback, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *MessageTTL) void {
        self.expiration_manager.deinit();

        var iterator = self.message_callbacks.iterator();
        while (iterator.next()) |entry| {
            self.allocator.free(entry.value_ptr.queue_name);
        }
        self.message_callbacks.deinit();
    }

    pub fn addMessage(self: *MessageTTL, queue_name: []const u8, message_id: u64, ttl_seconds: u32, expiry_callback: *const fn (queue_name: []const u8, message_id: u64) void) !u64 {
        const callback_data = MessageCallback{
            .queue_name = try self.allocator.dupe(u8, queue_name),
            .message_id = message_id,
            .expiry_callback = expiry_callback,
        };

        const expiration_id = try self.expiration_manager.add(ttl_seconds, messageExpiredCallback, @ptrCast(&callback_data));
        try self.message_callbacks.put(expiration_id, callback_data);

        return expiration_id;
    }

    pub fn removeMessage(self: *MessageTTL, expiration_id: u64) void {
        if (self.expiration_manager.remove(expiration_id)) {
            if (self.message_callbacks.fetchRemove(expiration_id)) |entry| {
                self.allocator.free(entry.value.queue_name);
            }
        }
    }

    pub fn processExpiredMessages(self: *MessageTTL) u32 {
        return self.expiration_manager.processExpired();
    }

    pub fn getNextExpirationTime(self: *MessageTTL) ?i64 {
        return self.expiration_manager.getNextExpirationTime();
    }

    pub fn getActiveMessageCount(self: *MessageTTL) usize {
        return self.expiration_manager.getCount();
    }

    fn messageExpiredCallback(id: u64, data: ?*anyopaque) void {
        const callback_data: *MessageCallback = @ptrCast(@alignCast(data));
        callback_data.expiry_callback(callback_data.queue_name, callback_data.message_id);
    }
};

/// Connection timeout manager
pub const ConnectionTimeout = struct {
    expiration_manager: ExpirationManager,
    connection_callbacks: std.HashMap(u64, ConnectionCallback, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage),
    allocator: std.mem.Allocator,

    const ConnectionCallback = struct {
        connection_id: u64,
        timeout_callback: *const fn (connection_id: u64) void,
    };

    pub fn init(allocator: std.mem.Allocator) ConnectionTimeout {
        return ConnectionTimeout{
            .expiration_manager = ExpirationManager.init(allocator),
            .connection_callbacks = std.HashMap(u64, ConnectionCallback, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ConnectionTimeout) void {
        self.expiration_manager.deinit();
        self.connection_callbacks.deinit();
    }

    pub fn addConnection(self: *ConnectionTimeout, connection_id: u64, timeout_seconds: u32, timeout_callback: *const fn (connection_id: u64) void) !u64 {
        const callback_data = ConnectionCallback{
            .connection_id = connection_id,
            .timeout_callback = timeout_callback,
        };

        const expiration_id = try self.expiration_manager.add(timeout_seconds, connectionTimeoutCallback, @ptrCast(&callback_data));
        try self.connection_callbacks.put(expiration_id, callback_data);

        return expiration_id;
    }

    pub fn refreshConnection(self: *ConnectionTimeout, expiration_id: u64, timeout_seconds: u32) !u64 {
        if (self.connection_callbacks.get(expiration_id)) |callback_data| {
            self.removeConnection(expiration_id);
            return try self.addConnection(callback_data.connection_id, timeout_seconds, callback_data.timeout_callback);
        }
        return error.ConnectionNotFound;
    }

    pub fn removeConnection(self: *ConnectionTimeout, expiration_id: u64) void {
        if (self.expiration_manager.remove(expiration_id)) {
            _ = self.connection_callbacks.remove(expiration_id);
        }
    }

    pub fn processExpiredConnections(self: *ConnectionTimeout) u32 {
        return self.expiration_manager.processExpired();
    }

    fn connectionTimeoutCallback(id: u64, data: ?*anyopaque) void {
        const callback_data: *ConnectionCallback = @ptrCast(@alignCast(data));
        callback_data.timeout_callback(callback_data.connection_id);
    }
};

/// Time utilities
pub const TimeUtils = struct {
    pub fn getCurrentTimestamp() i64 {
        return std.time.timestamp();
    }

    pub fn getCurrentMilliseconds() i64 {
        return std.time.milliTimestamp();
    }

    pub fn getCurrentMicroseconds() i64 {
        return std.time.microTimestamp();
    }

    pub fn formatTimestamp(timestamp: i64, allocator: std.mem.Allocator) ![]u8 {
        const epoch_seconds = @as(u64, @intCast(timestamp));
        const datetime = std.time.epoch.EpochSeconds{ .secs = epoch_seconds };
        const day_seconds = datetime.getDaySeconds();
        const year_day = datetime.getEpochDay().calculateYearDay();
        const month_day = year_day.calculateMonthDay();

        return try std.fmt.allocPrint(allocator, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{ year_day.year, month_day.month.numeric(), month_day.day_index + 1, day_seconds.getHoursIntoDay(), day_seconds.getMinutesIntoHour(), day_seconds.getSecondsIntoMinute() });
    }

    pub fn secondsUntil(target_timestamp: i64) i64 {
        return target_timestamp - std.time.timestamp();
    }

    pub fn millisecondsUntil(target_timestamp: i64) i64 {
        return target_timestamp - std.time.milliTimestamp();
    }
};

test "expiration manager basic operations" {
    const allocator = std.testing.allocator;

    var manager = ExpirationManager.init(allocator);
    defer manager.deinit();

    try std.testing.expectEqual(@as(usize, 0), manager.getCount());
    try std.testing.expectEqual(@as(?i64, null), manager.getNextExpirationTime());

    // Test data for callback
    const TestData = struct {
        var expired_ids: std.ArrayList(u64) = undefined;
        var allocator_ref: std.mem.Allocator = undefined;

        fn init(alloc: std.mem.Allocator) void {
            allocator_ref = alloc;
            expired_ids = std.ArrayList(u64).init(alloc);
        }

        fn deinit() void {
            expired_ids.deinit();
        }

        fn callback(id: u64, data: ?*anyopaque) void {
            _ = data;
            expired_ids.append(id) catch unreachable;
        }
    };

    TestData.init(allocator);
    defer TestData.deinit();

    // Add an entry that expires in 1 second
    const id1 = try manager.add(1, TestData.callback, null);
    try std.testing.expectEqual(@as(usize, 1), manager.getCount());

    // Add an entry that expires in 2 seconds
    const id2 = try manager.add(2, TestData.callback, null);
    try std.testing.expectEqual(@as(usize, 2), manager.getCount());

    // Process expired entries (should be none yet)
    const expired_count = manager.processExpired();
    try std.testing.expectEqual(@as(u32, 0), expired_count);

    // Remove an entry
    try std.testing.expectEqual(true, manager.remove(id1));
    try std.testing.expectEqual(@as(usize, 1), manager.getCount());

    // Try to remove non-existent entry
    try std.testing.expectEqual(false, manager.remove(999));
}

test "time utils" {
    const allocator = std.testing.allocator;

    const now = TimeUtils.getCurrentTimestamp();
    try std.testing.expect(now > 0);

    const formatted = try TimeUtils.formatTimestamp(now, allocator);
    defer allocator.free(formatted);
    try std.testing.expect(formatted.len > 0);

    // Test seconds until
    const future = now + 60;
    const seconds_until = TimeUtils.secondsUntil(future);
    try std.testing.expect(seconds_until > 55 and seconds_until <= 60);
}
