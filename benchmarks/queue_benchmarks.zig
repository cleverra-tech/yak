const std = @import("std");
const Queue = @import("yak").queue.Queue;
const Message = @import("yak").message.Message;
const Consumer = @import("yak").consumer.Consumer;

pub fn benchmarkQueuePublish(allocator: std.mem.Allocator, iteration: u64) !void {
    var queue = try Queue.init(allocator, "benchmark.queue", true, false, false, null);
    defer queue.deinit();
    
    var message = try Message.init(allocator, iteration, "test.exchange", "test.key", "Benchmark message for publish test");
    defer message.deinit();
    
    try queue.publish(message);
}

pub fn benchmarkQueueConsume(allocator: std.mem.Allocator, _: u64) !void {
    var queue = try Queue.init(allocator, "benchmark.queue", true, false, false, null);
    defer queue.deinit();
    
    // Pre-populate queue with messages
    for (0..10) |i| {
        const message = try Message.init(allocator, i, "test.exchange", "test.key", "Benchmark message");
        try queue.publish(message);
    }
    
    // Consumer for benchmark
    var consumer = try Consumer.init(allocator, "benchmark-consumer", "benchmark.queue", 1, 1, false, false, false, false, null);
    defer consumer.deinit();
    
    try queue.consume(consumer);
    
    // Get message if available
    if (try queue.get(false)) |message| {
        var msg = message;
        defer msg.deinit();
    }
}

pub fn benchmarkQueueAcknowledge(allocator: std.mem.Allocator, _: u64) !void {
    var queue = try Queue.init(allocator, "benchmark.queue", true, false, false, null);
    defer queue.deinit();
    
    // Pre-populate and get messages to have delivery tags
    for (0..5) |i| {
        const message = try Message.init(allocator, i, "test.exchange", "test.key", "Ack test message");
        try queue.publish(message);
    }
    
    // Get a message to acknowledge
    if (try queue.get(false)) |message| {
        var msg = message;
        defer msg.deinit();
        
        // Acknowledge the message
        try queue.acknowledge(msg.id, false);
    }
}

pub fn benchmarkQueueReject(allocator: std.mem.Allocator, _: u64) !void {
    var queue = try Queue.init(allocator, "benchmark.queue", true, false, false, null);
    defer queue.deinit();
    
    // Pre-populate queue
    for (0..5) |i| {
        const message = try Message.init(allocator, i, "test.exchange", "test.key", "Reject test message");
        try queue.publish(message);
    }
    
    // Get a message to reject
    if (try queue.get(false)) |message| {
        var msg = message;
        defer msg.deinit();
        
        // Reject the message (don't requeue to avoid infinite loop)
        try queue.reject(msg.id, false);
    }
}

pub fn benchmarkQueuePurge(allocator: std.mem.Allocator, iteration: u64) !void {
    _ = iteration;
    
    var queue = try Queue.init(allocator, "benchmark.queue", true, false, false, null);
    defer queue.deinit();
    
    // Pre-populate queue with many messages
    for (0..1000) |i| {
        const message = try Message.init(allocator, i, "test.exchange", "test.key", "Purge test message");
        try queue.publish(message);
    }
    
    // Purge all messages
    _ = queue.purge();
}