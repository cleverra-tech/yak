const std = @import("std");
const Exchange = @import("yak").exchange.Exchange;
const ExchangeType = @import("yak").exchange.ExchangeType;
const Message = @import("yak").message.Message;

pub fn benchmarkDirectExchangeRouting(allocator: std.mem.Allocator, iteration: u64) !void {
    var exchange = try Exchange.init(allocator, "direct.exchange", .direct, true, false, false, null);
    defer exchange.deinit();
    
    // Set up bindings
    try exchange.bindQueue("queue1", "key1", null);
    try exchange.bindQueue("queue2", "key2", null);
    try exchange.bindQueue("queue3", "key1", null); // Multiple queues for same key
    
    var message = try Message.init(allocator, iteration, "direct.exchange", "key1", "Direct routing test message");
    defer message.deinit();
    
    const matched_queues = try exchange.routeMessage(&message, allocator);
    defer allocator.free(matched_queues);
}

pub fn benchmarkFanoutExchangeRouting(allocator: std.mem.Allocator, iteration: u64) !void {
    var exchange = try Exchange.init(allocator, "fanout.exchange", .fanout, true, false, false, null);
    defer exchange.deinit();
    
    // Set up multiple bindings (routing key ignored for fanout)
    try exchange.bindQueue("queue1", "", null);
    try exchange.bindQueue("queue2", "", null);
    try exchange.bindQueue("queue3", "", null);
    try exchange.bindQueue("queue4", "", null);
    try exchange.bindQueue("queue5", "", null);
    
    var message = try Message.init(allocator, iteration, "fanout.exchange", "any.key", "Fanout routing test message");
    defer message.deinit();
    
    const matched_queues = try exchange.routeMessage(&message, allocator);
    defer allocator.free(matched_queues);
}

pub fn benchmarkTopicExchangeRouting(allocator: std.mem.Allocator, iteration: u64) !void {
    var exchange = try Exchange.init(allocator, "topic.exchange", .topic, true, false, false, null);
    defer exchange.deinit();
    
    // Set up topic patterns
    try exchange.bindQueue("logs_queue", "logs.*", null);
    try exchange.bindQueue("error_queue", "*.error", null);
    try exchange.bindQueue("all_queue", "#", null);
    try exchange.bindQueue("app_queue", "app.*.info", null);
    try exchange.bindQueue("system_queue", "system.#", null);
    
    const routing_keys = [_][]const u8{
        "logs.info",
        "app.error",
        "system.cpu.warning",
        "database.error",
        "app.service.info",
    };
    
    const key = routing_keys[iteration % routing_keys.len];
    var message = try Message.init(allocator, iteration, "topic.exchange", key, "Topic routing test message");
    defer message.deinit();
    
    const matched_queues = try exchange.routeMessage(&message, allocator);
    defer allocator.free(matched_queues);
}

pub fn benchmarkHeadersExchangeRouting(allocator: std.mem.Allocator, iteration: u64) !void {
    var exchange = try Exchange.init(allocator, "headers.exchange", .headers, true, false, false, null);
    defer exchange.deinit();
    
    // Set up header-based bindings
    try exchange.bindQueue("high_priority", "", "{\"x-match\":\"all\",\"priority\":\"high\",\"type\":\"order\"}");
    try exchange.bindQueue("any_error", "", "{\"x-match\":\"any\",\"level\":\"error\",\"type\":\"log\"}");
    try exchange.bindQueue("order_queue", "", "{\"x-match\":\"all\",\"type\":\"order\"}");
    
    var message = try Message.init(allocator, iteration, "headers.exchange", "test.key", "Headers routing test message");
    defer message.deinit();
    
    // Set message headers
    try message.setHeader("priority", "high");
    try message.setHeader("type", "order");
    try message.setHeader("region", "us-east");
    
    const matched_queues = try exchange.routeMessage(&message, allocator);
    defer allocator.free(matched_queues);
}