const std = @import("std");
const Message = @import("yak").message.Message;
const DeathReason = @import("yak").message.DeathReason;

pub fn benchmarkMessageCreation(allocator: std.mem.Allocator, iteration: u64) !void {
    var message = try Message.init(allocator, iteration, "test.exchange", "test.key", "Hello, World! This is a test message.");
    defer message.deinit();
}

pub fn benchmarkMessageSerialization(allocator: std.mem.Allocator, iteration: u64) !void {
    var message = try Message.init(allocator, iteration, "test.exchange", "test.key", "Hello, World! This is a test message for serialization.");
    defer message.deinit();

    try message.setHeader("content-type", "text/plain");
    try message.setHeader("priority", "high");
    message.markPersistent();

    const encoded = try message.encodeForStorage(allocator);
    defer allocator.free(encoded);
}

pub fn benchmarkMessageDeserialization(allocator: std.mem.Allocator, iteration: u64) !void {
    var original = try Message.init(allocator, iteration, "test.exchange", "test.key", "Hello, World! This is a test message for deserialization.");
    defer original.deinit();

    try original.setHeader("content-type", "application/json");
    original.markPersistent();

    const encoded = try original.encodeForStorage(allocator);
    defer allocator.free(encoded);

    var decoded = try Message.decodeFromStorage(encoded, allocator);
    defer decoded.deinit();
}

pub fn benchmarkMessageHeaders(allocator: std.mem.Allocator, iteration: u64) !void {
    var message = try Message.init(allocator, iteration, "test.exchange", "test.key", "Header test message");
    defer message.deinit();

    // Set multiple headers
    try message.setHeader("content-type", "application/json");
    try message.setHeader("user-id", "test-user");
    try message.setHeader("priority", "high");
    try message.setHeader("correlation-id", "12345");
    try message.setHeader("reply-to", "response.queue");

    // Get headers
    _ = message.getHeader("content-type");
    _ = message.getHeader("user-id");
    _ = message.getHeader("priority");
    _ = message.getHeader("correlation-id");
    _ = message.getHeader("reply-to");
}

pub fn benchmarkMessageClone(allocator: std.mem.Allocator, iteration: u64) !void {
    var original = try Message.init(allocator, iteration, "test.exchange", "test.key", "Clone test message with headers and properties");
    defer original.deinit();

    try original.setHeader("content-type", "text/plain");
    try original.setHeader("user-id", "clone-user");
    original.markPersistent();
    original.incrementDeliveryCount();

    var cloned = try original.clone(allocator);
    defer cloned.deinit();
}

pub fn benchmarkDeadLetterOperations(allocator: std.mem.Allocator, iteration: u64) !void {
    var message = try Message.init(allocator, iteration, "test.exchange", "test.key", "Dead letter test message");
    defer message.deinit();

    // Add death events
    const routing_keys = [_][]const u8{"key1"};
    const routing_keys_slice: [][]const u8 = @constCast(&routing_keys);
    try message.addDeathEvent("source.queue", .rejected, "source.exchange", routing_keys_slice);
    try message.addDeathEvent("source.queue", .delivery_limit, "source.exchange", routing_keys_slice);

    // Check dead letter conditions
    _ = message.shouldDeadLetterForDeliveryCount();

    // Create dead letter message
    var dead_letter = try message.createDeadLetterMessage(allocator, "dlx.exchange", "dlx.key");
    defer dead_letter.deinit();
}
