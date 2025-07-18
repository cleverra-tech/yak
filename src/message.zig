const std = @import("std");

pub const HeaderTable = std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage);

pub const CompressionType = enum {
    none,
    gzip,
    zlib,
    
    pub fn fromString(str: []const u8) CompressionType {
        if (std.mem.eql(u8, str, "gzip")) return .gzip;
        if (std.mem.eql(u8, str, "zlib")) return .zlib;
        return .none;
    }
    
    pub fn toString(self: CompressionType) []const u8 {
        return switch (self) {
            .none => "none",
            .gzip => "gzip",
            .zlib => "zlib",
        };
    }
};

pub const DeathReason = enum {
    rejected,
    expired,
    maxlen,
    maxlen_bytes,
    delivery_limit,
};

pub const DeathEvent = struct {
    queue: []const u8,
    reason: DeathReason,
    exchange: []const u8,
    routing_keys: [][]const u8,
    count: u32,
    time: i64,

    pub fn deinit(self: *DeathEvent, allocator: std.mem.Allocator) void {
        allocator.free(self.queue);
        allocator.free(self.exchange);
        for (self.routing_keys) |key| {
            allocator.free(key);
        }
        allocator.free(self.routing_keys);
    }
};

pub const Message = struct {
    id: u64,
    exchange: []const u8,
    routing_key: []const u8,
    body: []const u8,
    headers: ?HeaderTable,
    persistent: bool,
    delivery_count: u32,
    timestamp: i64,

    // Compression support
    compression_type: CompressionType,
    original_size: ?usize, // Size before compression
    is_compressed: bool,

    // Dead letter queue support
    deaths: std.ArrayList(DeathEvent),
    first_death_queue: ?[]const u8,
    first_death_reason: ?DeathReason,
    first_death_exchange: ?[]const u8,
    max_delivery_count: ?u32,

    // Wombat storage metadata
    wombat_key: []const u8,
    // value_pointer: ?wombat.ValuePointer, // Commented out until Wombat is available

    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, id: u64, exchange: []const u8, routing_key: []const u8, body: []const u8) !Message {
        const timestamp = std.time.timestamp();

        return Message{
            .id = id,
            .exchange = try allocator.dupe(u8, exchange),
            .routing_key = try allocator.dupe(u8, routing_key),
            .body = try allocator.dupe(u8, body),
            .headers = null,
            .persistent = false,
            .delivery_count = 0,
            .timestamp = timestamp,
            .compression_type = .none,
            .original_size = null,
            .is_compressed = false,
            .deaths = std.ArrayList(DeathEvent).init(allocator),
            .first_death_queue = null,
            .first_death_reason = null,
            .first_death_exchange = null,
            .max_delivery_count = null,
            .wombat_key = &[_]u8{},
            // .value_pointer = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Message) void {
        self.allocator.free(self.exchange);
        self.allocator.free(self.routing_key);
        self.allocator.free(self.body);

        if (self.headers) |*headers| {
            var iterator = headers.iterator();
            while (iterator.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                self.allocator.free(entry.value_ptr.*);
            }
            headers.deinit();
        }

        if (self.wombat_key.len > 0) {
            self.allocator.free(self.wombat_key);
        }

        // Clean up death events
        for (self.deaths.items) |*death| {
            death.deinit(self.allocator);
        }
        self.deaths.deinit();

        // Clean up first death fields
        if (self.first_death_queue) |queue| {
            self.allocator.free(queue);
        }
        if (self.first_death_exchange) |exchange| {
            self.allocator.free(exchange);
        }
    }

    pub fn setHeader(self: *Message, key: []const u8, value: []const u8) !void {
        if (self.headers == null) {
            self.headers = HeaderTable.init(self.allocator);
        }

        const owned_key = try self.allocator.dupe(u8, key);
        const owned_value = try self.allocator.dupe(u8, value);

        try self.headers.?.put(owned_key, owned_value);
    }

    pub fn getHeader(self: *const Message, key: []const u8) ?[]const u8 {
        if (self.headers) |headers| {
            return headers.get(key);
        }
        return null;
    }

    pub fn encodeForStorage(self: *const Message, allocator: std.mem.Allocator) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();

        try buffer.writer().writeInt(u64, self.id, .little);
        try buffer.writer().writeInt(u64, @intCast(self.timestamp), .little);
        try buffer.writer().writeInt(u32, self.delivery_count, .little);
        try buffer.writer().writeInt(u8, if (self.persistent) 1 else 0, .little);

        // Write compression fields
        try buffer.writer().writeInt(u8, @intFromEnum(self.compression_type), .little);
        try buffer.writer().writeInt(u8, if (self.is_compressed) 1 else 0, .little);
        if (self.original_size) |size| {
            try buffer.writer().writeInt(u8, 1, .little); // has_original_size flag
            try buffer.writer().writeInt(u64, size, .little);
        } else {
            try buffer.writer().writeInt(u8, 0, .little); // no original_size
        }

        // Write variable-length fields
        try writeString(buffer.writer(), self.exchange);
        try writeString(buffer.writer(), self.routing_key);
        try writeString(buffer.writer(), self.body);

        // Write headers if present
        if (self.headers) |headers| {
            try buffer.writer().writeInt(u8, 1, .little); // has_headers flag
            try writeHeaders(buffer.writer(), headers);
        } else {
            try buffer.writer().writeInt(u8, 0, .little); // no headers
        }

        return buffer.toOwnedSlice();
    }

    pub fn decodeFromStorage(data: []const u8, allocator: std.mem.Allocator) !Message {
        var stream = std.io.fixedBufferStream(data);
        var reader = stream.reader();

        const id = try reader.readInt(u64, .little);
        const timestamp = try reader.readInt(u64, .little);
        const delivery_count = try reader.readInt(u32, .little);
        const persistent = (try reader.readInt(u8, .little)) != 0;

        // Read compression fields
        const compression_type = @as(CompressionType, @enumFromInt(try reader.readInt(u8, .little)));
        const is_compressed = (try reader.readInt(u8, .little)) != 0;
        const has_original_size = (try reader.readInt(u8, .little)) != 0;
        const original_size = if (has_original_size) try reader.readInt(u64, .little) else null;

        const exchange = try readString(reader, allocator);
        const routing_key = try readString(reader, allocator);
        const body = try readString(reader, allocator);

        // Read headers if present
        const has_headers = (try reader.readInt(u8, .little)) != 0;
        const headers = if (has_headers) try readHeaders(reader, allocator) else null;

        return Message{
            .id = id,
            .exchange = exchange,
            .routing_key = routing_key,
            .body = body,
            .headers = headers,
            .persistent = persistent,
            .delivery_count = delivery_count,
            .timestamp = @intCast(timestamp),
            .compression_type = compression_type,
            .original_size = original_size,
            .is_compressed = is_compressed,
            .deaths = std.ArrayList(DeathEvent).init(allocator),
            .first_death_queue = null,
            .first_death_reason = null,
            .first_death_exchange = null,
            .max_delivery_count = null,
            .wombat_key = &[_]u8{},
            // .value_pointer = null,
            .allocator = allocator,
        };
    }

    pub fn incrementDeliveryCount(self: *Message) void {
        self.delivery_count += 1;
    }

    pub fn markPersistent(self: *Message) void {
        self.persistent = true;
    }

    pub fn clone(self: *const Message, allocator: std.mem.Allocator) !Message {
        var cloned = try Message.init(allocator, self.id, self.exchange, self.routing_key, self.body);
        cloned.persistent = self.persistent;
        cloned.delivery_count = self.delivery_count;
        cloned.timestamp = self.timestamp;
        cloned.compression_type = self.compression_type;
        cloned.original_size = self.original_size;
        cloned.is_compressed = self.is_compressed;

        if (self.headers) |headers| {
            cloned.headers = HeaderTable.init(allocator);
            var iterator = headers.iterator();
            while (iterator.next()) |entry| {
                try cloned.setHeader(entry.key_ptr.*, entry.value_ptr.*);
            }
        }

        // Copy death information
        cloned.max_delivery_count = self.max_delivery_count;
        if (self.first_death_queue) |queue| {
            cloned.first_death_queue = try allocator.dupe(u8, queue);
        }
        if (self.first_death_exchange) |exchange| {
            cloned.first_death_exchange = try allocator.dupe(u8, exchange);
        }
        cloned.first_death_reason = self.first_death_reason;

        // Copy death events
        for (self.deaths.items) |*death| {
            var cloned_keys = try allocator.alloc([]const u8, death.routing_keys.len);
            for (death.routing_keys, 0..) |key, i| {
                cloned_keys[i] = try allocator.dupe(u8, key);
            }

            const cloned_death = DeathEvent{
                .queue = try allocator.dupe(u8, death.queue),
                .reason = death.reason,
                .exchange = try allocator.dupe(u8, death.exchange),
                .routing_keys = cloned_keys,
                .count = death.count,
                .time = death.time,
            };
            try cloned.deaths.append(cloned_death);
        }

        return cloned;
    }

    /// Add a death event to this message
    pub fn addDeathEvent(self: *Message, queue: []const u8, reason: DeathReason, exchange: []const u8, routing_keys: [][]const u8) !void {
        // Set first death information if not already set
        if (self.first_death_queue == null) {
            self.first_death_queue = try self.allocator.dupe(u8, queue);
            self.first_death_reason = reason;
            self.first_death_exchange = try self.allocator.dupe(u8, exchange);
        }

        // Check if we already have a death event for this queue and reason
        for (self.deaths.items) |*death| {
            if (std.mem.eql(u8, death.queue, queue) and death.reason == reason) {
                death.count += 1;
                death.time = std.time.timestamp();
                return;
            }
        }

        // Create new death event
        var cloned_keys = try self.allocator.alloc([]const u8, routing_keys.len);
        for (routing_keys, 0..) |key, i| {
            cloned_keys[i] = try self.allocator.dupe(u8, key);
        }

        const death_event = DeathEvent{
            .queue = try self.allocator.dupe(u8, queue),
            .reason = reason,
            .exchange = try self.allocator.dupe(u8, exchange),
            .routing_keys = cloned_keys,
            .count = 1,
            .time = std.time.timestamp(),
        };

        try self.deaths.append(death_event);
    }

    /// Check if message should be dead lettered based on delivery count
    pub fn shouldDeadLetterForDeliveryCount(self: *const Message) bool {
        if (self.max_delivery_count) |max_count| {
            return self.delivery_count >= max_count;
        }
        return false;
    }

    /// Create a dead letter message with death headers
    pub fn createDeadLetterMessage(self: *const Message, allocator: std.mem.Allocator, dl_exchange: []const u8, dl_routing_key: ?[]const u8) !Message {
        var dead_letter = try self.clone(allocator);

        // Override exchange and routing key for dead letter routing
        allocator.free(dead_letter.exchange);
        dead_letter.exchange = try allocator.dupe(u8, dl_exchange);

        if (dl_routing_key) |routing_key| {
            allocator.free(dead_letter.routing_key);
            dead_letter.routing_key = try allocator.dupe(u8, routing_key);
        }

        // Add death headers to the message
        try dead_letter.addDeathHeaders();

        return dead_letter;
    }

    /// Add AMQP death headers to the message
    fn addDeathHeaders(self: *Message) !void {
        // Add x-death header with death events
        if (self.deaths.items.len > 0) {
            // In a real implementation, this would serialize the death events as AMQP arrays
            // For now, we'll add basic death information
            try self.setHeader("x-death", "present");
        }

        // Add first death information
        if (self.first_death_queue) |queue| {
            try self.setHeader("x-first-death-queue", queue);
        }
        if (self.first_death_exchange) |exchange| {
            try self.setHeader("x-first-death-exchange", exchange);
        }
        if (self.first_death_reason) |reason| {
            const reason_str = switch (reason) {
                .rejected => "rejected",
                .expired => "expired",
                .maxlen => "maxlen",
                .maxlen_bytes => "maxlen_bytes",
                .delivery_limit => "delivery_limit",
            };
            try self.setHeader("x-first-death-reason", reason_str);
        }
    }

    // Compression configuration
    pub const DEFAULT_COMPRESSION_THRESHOLD: usize = 1024; // 1KB

    /// Compresses the message body if it exceeds the threshold
    pub fn compressIfNeeded(self: *Message, compression_type: CompressionType, threshold: usize) !void {
        if (self.is_compressed or self.body.len < threshold or compression_type == .none) {
            return;
        }

        const compressed_body = try compressData(self.allocator, self.body, compression_type);
        errdefer self.allocator.free(compressed_body);

        // Only compress if we actually save space (add some overhead tolerance)
        if (compressed_body.len < self.body.len - 32) {
            self.allocator.free(self.body);
            self.original_size = self.body.len;
            self.body = compressed_body;
            self.compression_type = compression_type;
            self.is_compressed = true;
            
            // Set compression headers
            try self.setHeader("content-encoding", compression_type.toString());
            const original_size_str = try std.fmt.allocPrint(self.allocator, "{}", .{self.original_size.?});
            try self.setHeader("x-original-size", original_size_str);
        } else {
            // Compression didn't save enough space, keep original
            self.allocator.free(compressed_body);
        }
    }

    /// Decompresses the message body if it's compressed
    pub fn decompress(self: *Message) !void {
        if (!self.is_compressed) {
            return;
        }

        const decompressed_body = try decompressData(self.allocator, self.body, self.compression_type);
        errdefer self.allocator.free(decompressed_body);

        self.allocator.free(self.body);
        self.body = decompressed_body;
        self.is_compressed = false;
        self.compression_type = .none;
        self.original_size = null;

        // Remove compression headers
        _ = self.removeHeader("content-encoding");
        _ = self.removeHeader("x-original-size");
    }

    /// Gets the effective body size (uncompressed size if compressed)
    pub fn getEffectiveSize(self: *const Message) usize {
        if (self.is_compressed and self.original_size != null) {
            return self.original_size.?;
        }
        return self.body.len;
    }

    /// Gets compression ratio as a percentage (0-100)
    pub fn getCompressionRatio(self: *const Message) ?f32 {
        if (!self.is_compressed or self.original_size == null) {
            return null;
        }
        const ratio = @as(f32, @floatFromInt(self.body.len)) / @as(f32, @floatFromInt(self.original_size.?));
        return ratio * 100.0;
    }

    /// Removes a header and returns the old value if it existed
    pub fn removeHeader(self: *Message, key: []const u8) ?[]const u8 {
        if (self.headers == null) return null;
        
        var headers = &self.headers.?;
        if (headers.fetchRemove(key)) |kv| {
            self.allocator.free(kv.key);
            const value = kv.value;
            return value;
        }
        return null;
    }
};

/// Compresses data using the specified compression type
fn compressData(allocator: std.mem.Allocator, data: []const u8, compression_type: CompressionType) ![]u8 {
    switch (compression_type) {
        .none => return try allocator.dupe(u8, data),
        .gzip => {
            var compressed = std.ArrayList(u8).init(allocator);
            errdefer compressed.deinit();
            
            var compressor = try std.compress.gzip.compressor(compressed.writer(), .{});
            try compressor.writer().writeAll(data);
            try compressor.finish();
            
            return try compressed.toOwnedSlice();
        },
        .zlib => {
            var compressed = std.ArrayList(u8).init(allocator);
            errdefer compressed.deinit();
            
            var compressor = try std.compress.zlib.compressor(compressed.writer(), .{});
            try compressor.writer().writeAll(data);
            try compressor.finish();
            
            return try compressed.toOwnedSlice();
        },
    }
}

/// Decompresses data using the specified compression type
fn decompressData(allocator: std.mem.Allocator, compressed_data: []const u8, compression_type: CompressionType) ![]u8 {
    switch (compression_type) {
        .none => return try allocator.dupe(u8, compressed_data),
        .gzip => {
            var fbs = std.io.fixedBufferStream(compressed_data);
            var decompressor = std.compress.gzip.decompressor(fbs.reader());
            
            var decompressed = std.ArrayList(u8).init(allocator);
            errdefer decompressed.deinit();
            
            try decompressor.reader().readAllArrayList(&decompressed, std.math.maxInt(usize));
            return try decompressed.toOwnedSlice();
        },
        .zlib => {
            var fbs = std.io.fixedBufferStream(compressed_data);
            var decompressor = std.compress.zlib.decompressor(fbs.reader());
            
            var decompressed = std.ArrayList(u8).init(allocator);
            errdefer decompressed.deinit();
            
            try decompressor.reader().readAllArrayList(&decompressed, std.math.maxInt(usize));
            return try decompressed.toOwnedSlice();
        },
    }
}

// Helper functions for serialization
fn writeString(writer: anytype, str: []const u8) !void {
    try writer.writeInt(u32, @intCast(str.len), .little);
    try writer.writeAll(str);
}

fn readString(reader: anytype, allocator: std.mem.Allocator) ![]u8 {
    const len = try reader.readInt(u32, .little);
    const str = try allocator.alloc(u8, len);
    try reader.readNoEof(str);
    return str;
}

fn writeHeaders(writer: anytype, headers: HeaderTable) !void {
    try writer.writeInt(u32, @intCast(headers.count()), .little);
    var iterator = headers.iterator();
    while (iterator.next()) |entry| {
        try writeString(writer, entry.key_ptr.*);
        try writeString(writer, entry.value_ptr.*);
    }
}

fn readHeaders(reader: anytype, allocator: std.mem.Allocator) !HeaderTable {
    const count = try reader.readInt(u32, .little);
    var headers = HeaderTable.init(allocator);

    for (0..count) |_| {
        const key = try readString(reader, allocator);
        const value = try readString(reader, allocator);
        try headers.put(key, value);
    }

    return headers;
}

test "message creation and manipulation" {
    const allocator = std.testing.allocator;

    var message = try Message.init(allocator, 1, "test.exchange", "test.key", "Hello, World!");
    defer message.deinit();

    try std.testing.expectEqual(@as(u64, 1), message.id);
    try std.testing.expectEqualStrings("test.exchange", message.exchange);
    try std.testing.expectEqualStrings("test.key", message.routing_key);
    try std.testing.expectEqualStrings("Hello, World!", message.body);
    try std.testing.expectEqual(@as(u32, 0), message.delivery_count);
    try std.testing.expectEqual(false, message.persistent);

    message.incrementDeliveryCount();
    try std.testing.expectEqual(@as(u32, 1), message.delivery_count);

    message.markPersistent();
    try std.testing.expectEqual(true, message.persistent);
}

test "message headers" {
    const allocator = std.testing.allocator;

    var message = try Message.init(allocator, 1, "test.exchange", "test.key", "Hello, World!");
    defer message.deinit();

    try message.setHeader("content-type", "text/plain");
    try message.setHeader("user-id", "test-user");

    try std.testing.expectEqualStrings("text/plain", message.getHeader("content-type").?);
    try std.testing.expectEqualStrings("test-user", message.getHeader("user-id").?);
    try std.testing.expectEqual(@as(?[]const u8, null), message.getHeader("non-existent"));
}

test "message serialization" {
    const allocator = std.testing.allocator;

    var original = try Message.init(allocator, 42, "test.exchange", "test.key", "Test message body");
    defer original.deinit();

    try original.setHeader("content-type", "application/json");
    original.markPersistent();
    original.incrementDeliveryCount();

    const encoded = try original.encodeForStorage(allocator);
    defer allocator.free(encoded);

    var decoded = try Message.decodeFromStorage(encoded, allocator);
    defer decoded.deinit();

    try std.testing.expectEqual(original.id, decoded.id);
    try std.testing.expectEqualStrings(original.exchange, decoded.exchange);
    try std.testing.expectEqualStrings(original.routing_key, decoded.routing_key);
    try std.testing.expectEqualStrings(original.body, decoded.body);
    try std.testing.expectEqual(original.persistent, decoded.persistent);
    try std.testing.expectEqual(original.delivery_count, decoded.delivery_count);
    try std.testing.expectEqualStrings("application/json", decoded.getHeader("content-type").?);
}

test "message compression and decompression" {
    const allocator = std.testing.allocator;

    // Create a large message to test compression
    const large_body = "This is a large message body that should be compressed when it exceeds the threshold. " ++
                      "We repeat this text multiple times to ensure it's large enough for compression to be effective. " ++
                      "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut " ++
                      "labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco " ++
                      "laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in " ++
                      "voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat " ++
                      "non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. " ++
                      "This text is repeated to make the message large enough for compression to be worthwhile.";

    var message = try Message.init(allocator, 1, "test.exchange", "test.key", large_body);
    defer message.deinit();

    const original_size = message.body.len;

    // Test compression
    try message.compressIfNeeded(.gzip, 100); // Low threshold to force compression
    
    try std.testing.expect(message.is_compressed);
    try std.testing.expectEqual(CompressionType.gzip, message.compression_type);
    try std.testing.expectEqual(original_size, message.original_size.?);
    try std.testing.expect(message.body.len < original_size); // Compressed should be smaller

    // Test compression headers
    try std.testing.expectEqualStrings("gzip", message.getHeader("content-encoding").?);
    
    // Test getting effective size
    try std.testing.expectEqual(original_size, message.getEffectiveSize());
    
    // Test compression ratio
    const ratio = message.getCompressionRatio().?;
    try std.testing.expect(ratio > 0.0 and ratio < 100.0);

    // Test decompression
    try message.decompress();
    
    try std.testing.expect(!message.is_compressed);
    try std.testing.expectEqual(CompressionType.none, message.compression_type);
    try std.testing.expectEqual(@as(?usize, null), message.original_size);
    try std.testing.expectEqualStrings(large_body, message.body);
    try std.testing.expectEqual(@as(?[]const u8, null), message.getHeader("content-encoding"));
}

test "message compression threshold" {
    const allocator = std.testing.allocator;

    // Small message - should not be compressed
    var small_message = try Message.init(allocator, 1, "test.exchange", "test.key", "small");
    defer small_message.deinit();

    try small_message.compressIfNeeded(.gzip, 100);
    try std.testing.expect(!small_message.is_compressed);

    // Large message - should be compressed
    var large_message = try Message.init(allocator, 2, "test.exchange", "test.key", "a" ** 200);
    defer large_message.deinit();

    try large_message.compressIfNeeded(.gzip, 100);
    try std.testing.expect(large_message.is_compressed);
}
