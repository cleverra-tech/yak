const std = @import("std");
const Message = @import("yak").message.Message;

// Mock storage adapter for benchmarking
const MockStorageAdapter = struct {
    allocator: std.mem.Allocator,
    storage: std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),

    pub fn init(allocator: std.mem.Allocator) MockStorageAdapter {
        return MockStorageAdapter{
            .allocator = allocator,
            .storage = std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
        };
    }

    pub fn deinit(self: *MockStorageAdapter) void {
        var iterator = self.storage.iterator();
        while (iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.storage.deinit();
    }

    pub fn set(self: *MockStorageAdapter, key: []const u8, value: []const u8) !void {
        const owned_key = try self.allocator.dupe(u8, key);
        const owned_value = try self.allocator.dupe(u8, value);
        try self.storage.put(owned_key, owned_value);
    }

    pub fn get(self: *MockStorageAdapter, key: []const u8) !?[]const u8 {
        if (self.storage.get(key)) |value| {
            return try self.allocator.dupe(u8, value);
        }
        return null;
    }

    pub fn delete(self: *MockStorageAdapter, key: []const u8) !void {
        if (self.storage.fetchRemove(key)) |entry| {
            self.allocator.free(entry.key);
            self.allocator.free(entry.value);
        }
    }

    pub fn generateMessageKey(self: *MockStorageAdapter, allocator: std.mem.Allocator, queue_name: []const u8, message_id: u64) ![]const u8 {
        _ = self;
        return try std.fmt.allocPrint(allocator, "queue:{s}:msg:{}", .{ queue_name, message_id });
    }
};

pub fn benchmarkMessageStorageWrite(allocator: std.mem.Allocator, iteration: u64) !void {
    var storage = MockStorageAdapter.init(allocator);
    defer storage.deinit();

    var message = try Message.init(allocator, iteration, "test.exchange", "test.key", "Storage write benchmark message with some content");
    defer message.deinit();

    try message.setHeader("content-type", "application/json");
    try message.setHeader("user-id", "benchmark-user");
    message.markPersistent();

    const storage_key = try storage.generateMessageKey(allocator, "benchmark.queue", iteration);
    defer allocator.free(storage_key);

    const encoded_message = try message.encodeForStorage(allocator);
    defer allocator.free(encoded_message);

    try storage.set(storage_key, encoded_message);
}

pub fn benchmarkMessageStorageRead(allocator: std.mem.Allocator, iteration: u64) !void {
    var storage = MockStorageAdapter.init(allocator);
    defer storage.deinit();

    // Pre-populate storage
    var original = try Message.init(allocator, iteration, "test.exchange", "test.key", "Storage read benchmark message");
    defer original.deinit();

    try original.setHeader("content-type", "text/plain");
    original.markPersistent();

    const storage_key = try storage.generateMessageKey(allocator, "benchmark.queue", iteration);
    defer allocator.free(storage_key);

    const encoded = try original.encodeForStorage(allocator);
    defer allocator.free(encoded);

    try storage.set(storage_key, encoded);

    // Benchmark read operation
    if (try storage.get(storage_key)) |data| {
        defer allocator.free(data);

        var decoded = try Message.decodeFromStorage(data, allocator);
        defer decoded.deinit();
    }
}

pub fn benchmarkMessageStorageBatchWrite(allocator: std.mem.Allocator, iteration: u64) !void {
    var storage = MockStorageAdapter.init(allocator);
    defer storage.deinit();

    const batch_size = 100;

    // Create batch of messages
    for (0..batch_size) |i| {
        const msg_id = iteration * batch_size + i;
        var message = try Message.init(allocator, msg_id, "test.exchange", "test.key", "Batch write message");
        defer message.deinit();

        try message.setHeader("batch-id", try std.fmt.allocPrint(allocator, "{}", .{iteration}));
        message.markPersistent();

        const storage_key = try storage.generateMessageKey(allocator, "batch.queue", msg_id);
        defer allocator.free(storage_key);

        const encoded = try message.encodeForStorage(allocator);
        defer allocator.free(encoded);

        try storage.set(storage_key, encoded);
    }
}

pub fn benchmarkMessageStorageBatchRead(allocator: std.mem.Allocator, iteration: u64) !void {
    var storage = MockStorageAdapter.init(allocator);
    defer storage.deinit();

    const batch_size = 100;

    // Pre-populate storage with batch
    for (0..batch_size) |i| {
        const msg_id = iteration * batch_size + i;
        var message = try Message.init(allocator, msg_id, "test.exchange", "test.key", "Batch read message");
        defer message.deinit();

        const storage_key = try storage.generateMessageKey(allocator, "batch.queue", msg_id);
        defer allocator.free(storage_key);

        const encoded = try message.encodeForStorage(allocator);
        defer allocator.free(encoded);

        try storage.set(storage_key, encoded);
    }

    // Benchmark batch read
    for (0..batch_size) |i| {
        const msg_id = iteration * batch_size + i;
        const storage_key = try storage.generateMessageKey(allocator, "batch.queue", msg_id);
        defer allocator.free(storage_key);

        if (try storage.get(storage_key)) |data| {
            defer allocator.free(data);

            var decoded = try Message.decodeFromStorage(data, allocator);
            defer decoded.deinit();
        }
    }
}
