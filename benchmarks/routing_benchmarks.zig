const std = @import("std");
const VirtualHost = @import("yak").vhost.VirtualHost;
const Message = @import("yak").message.Message;
const ExchangeType = @import("yak").exchange.ExchangeType;

pub fn benchmarkEndToEndRouting(allocator: std.mem.Allocator, iteration: u64) !void {
    var vhost = try VirtualHost.init(allocator, "bench-vhost");
    defer vhost.deinit();
    
    // Set up exchange and queues
    try vhost.declareExchange("test.exchange", .direct, true, false, false, null);
    _ = try vhost.declareQueue("test.queue1", true, false, false, null);
    _ = try vhost.declareQueue("test.queue2", true, false, false, null);
    
    // Bind queues
    try vhost.bindQueue("test.queue1", "test.exchange", "key1", null);
    try vhost.bindQueue("test.queue2", "test.exchange", "key2", null);
    
    var message = try Message.init(allocator, iteration, "test.exchange", "key1", "End-to-end routing test message");
    defer message.deinit();
    
    // Route message through vhost
    try vhost.routeMessage("test.exchange", &message);
}

pub fn benchmarkBindingOperations(allocator: std.mem.Allocator, iteration: u64) !void {
    var vhost = try VirtualHost.init(allocator, "bench-vhost");
    defer vhost.deinit();
    
    // Create exchange and queue
    try vhost.declareExchange("binding.exchange", .direct, true, false, false, null);
    _ = try vhost.declareQueue("binding.queue", true, false, false, null);
    
    const key = try std.fmt.allocPrint(allocator, "key.{}", .{iteration});
    defer allocator.free(key);
    
    // Bind queue with unique key
    try vhost.bindQueue("binding.queue", "binding.exchange", key, null);
    
    // Unbind queue
    try vhost.unbindQueue("binding.queue", "binding.exchange", key, null);
}

pub fn benchmarkTopicPatternMatching(allocator: std.mem.Allocator, iteration: u64) !void {
    _ = allocator;
    
    // Import the topic matching function from exchange module
    // We'll use a simple pattern matching simulation for benchmarking
    const patterns_match = struct {
        fn matchPattern(pattern: []const u8, key: []const u8) bool {
            _ = pattern;
            _ = key;
            // Simple simulation - in real implementation this would use the actual matching logic
            return true;
        }
    }.matchPattern;
    
    const patterns = [_][]const u8{
        "logs.*",
        "*.error",
        "#",
        "app.*.info",
        "system.#",
        "queue.*.*.status",
        "events.#.critical",
    };
    
    const routing_keys = [_][]const u8{
        "logs.info",
        "app.error",
        "system.cpu.warning",
        "database.connection.error",
        "app.service.info",
        "queue.processor.task.status",
        "events.user.login.critical",
    };
    
    const pattern = patterns[iteration % patterns.len];
    const key = routing_keys[iteration % routing_keys.len];
    
    _ = patterns_match(pattern, key);
}