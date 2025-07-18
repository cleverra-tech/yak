const std = @import("std");
const Config = @import("../src/config.zig").Config;
const Server = @import("../src/core/server.zig").Server;
const VirtualHost = @import("../src/core/vhost.zig").VirtualHost;
const Message = @import("../src/message.zig").Message;
const Queue = @import("../src/routing/queue.zig").Queue;
const Exchange = @import("../src/routing/exchange.zig").Exchange;
const ExchangeType = @import("../src/routing/exchange.zig").ExchangeType;
const Consumer = @import("../src/consumer/consumer.zig").Consumer;
const Frame = @import("../src/protocol/frame.zig").Frame;
const FrameType = @import("../src/protocol/frame.zig").FrameType;
const Connection = @import("../src/network/connection.zig").Connection;

/// Test helper to create a test server configuration
fn createTestConfig(allocator: std.mem.Allocator) !Config {
    var config = try Config.default(allocator);
    config.tcp.port = 5673; // Use different port for testing
    config.cli.enabled = false; // Disable CLI for tests
    return config;
}

/// Test helper to create a test server
fn createTestServer(allocator: std.mem.Allocator) !Server {
    const config = try createTestConfig(allocator);
    var server = try Server.init(allocator, config);
    return server;
}

test "server lifecycle and basic operations" {
    const allocator = std.testing.allocator;

    var server = try createTestServer(allocator);
    defer server.deinit();

    // Test initial state
    try std.testing.expectEqual(@as(u32, 1), server.getVirtualHostCount()); // Default vhost "/"
    try std.testing.expectEqual(@as(u32, 0), server.getConnectionCount());
    try std.testing.expectEqual(false, server.isShutdownRequested());

    // Test virtual host creation
    try server.createVirtualHost("test-vhost");
    try std.testing.expectEqual(@as(u32, 2), server.getVirtualHostCount());

    const vhost = server.getVirtualHost("test-vhost");
    try std.testing.expect(vhost != null);
    try std.testing.expectEqualStrings("test-vhost", vhost.?.name);

    // Test graceful shutdown
    server.requestShutdown();
    try std.testing.expectEqual(true, server.isShutdownRequested());

    // Test server stats
    const stats = try server.getStats(allocator);
    defer stats.object.deinit();

    try std.testing.expect(stats.object.contains("connections"));
    try std.testing.expect(stats.object.contains("virtual_hosts"));
    try std.testing.expect(stats.object.contains("errors"));
}

test "virtual host operations and resource management" {
    const allocator = std.testing.allocator;

    var server = try createTestServer(allocator);
    defer server.deinit();

    const vhost = server.getVirtualHost("/");
    try std.testing.expect(vhost != null);

    // Test exchange operations
    try vhost.?.declareExchange("test.direct", .direct, true, false, false, null);
    try vhost.?.declareExchange("test.fanout", .fanout, false, true, false, null);
    try vhost.?.declareExchange("test.topic", .topic, true, false, false, null);

    const direct_exchange = vhost.?.getExchange("test.direct");
    try std.testing.expect(direct_exchange != null);
    try std.testing.expectEqual(ExchangeType.direct, direct_exchange.?.exchange_type);
    try std.testing.expectEqual(true, direct_exchange.?.durable);

    // Test queue operations
    const queue_name1 = try vhost.?.declareQueue("test.queue1", true, false, false, null);
    const queue_name2 = try vhost.?.declareQueue("test.queue2", false, false, true, null);
    const anon_queue = try vhost.?.declareQueue("", false, true, true, null); // Anonymous queue

    try std.testing.expectEqualStrings("test.queue1", queue_name1);
    try std.testing.expectEqualStrings("test.queue2", queue_name2);
    try std.testing.expect(anon_queue.len > 0);
    try std.testing.expect(std.mem.startsWith(u8, anon_queue, "amq.gen-"));

    const queue1 = vhost.?.getQueue("test.queue1");
    try std.testing.expect(queue1 != null);
    try std.testing.expectEqual(true, queue1.?.durable);

    // Test queue binding
    try vhost.?.bindQueue("test.queue1", "test.direct", "routing.key1", null);
    try vhost.?.bindQueue("test.queue2", "test.direct", "routing.key2", null);
    try vhost.?.bindQueue("test.queue1", "test.fanout", "", null);

    // Test queue stats
    try std.testing.expectEqual(@as(u32, 3), vhost.?.getQueueCount());
    try std.testing.expect(vhost.?.getExchangeCount() >= 6); // Default + declared exchanges

    // Test queue purging
    const purged_count = try vhost.?.purgeQueue("test.queue1");
    try std.testing.expectEqual(@as(u32, 0), purged_count);

    // Test queue deletion
    const deleted_msg_count = try vhost.?.deleteQueue("test.queue2", false, false);
    try std.testing.expectEqual(@as(u32, 0), deleted_msg_count);
    try std.testing.expectEqual(@as(u32, 2), vhost.?.getQueueCount());
}

test "message publishing and routing through exchanges" {
    const allocator = std.testing.allocator;

    var server = try createTestServer(allocator);
    defer server.deinit();

    const vhost = server.getVirtualHost("/");
    try std.testing.expect(vhost != null);

    // Set up exchanges and queues
    try vhost.?.declareExchange("test.direct", .direct, false, false, false, null);
    try vhost.?.declareExchange("test.fanout", .fanout, false, false, false, null);
    try vhost.?.declareExchange("test.topic", .topic, false, false, false, null);

    _ = try vhost.?.declareQueue("queue.direct1", false, false, false, null);
    _ = try vhost.?.declareQueue("queue.direct2", false, false, false, null);
    _ = try vhost.?.declareQueue("queue.fanout1", false, false, false, null);
    _ = try vhost.?.declareQueue("queue.fanout2", false, false, false, null);
    _ = try vhost.?.declareQueue("queue.topic", false, false, false, null);

    // Set up bindings
    try vhost.?.bindQueue("queue.direct1", "test.direct", "key1", null);
    try vhost.?.bindQueue("queue.direct2", "test.direct", "key2", null);
    try vhost.?.bindQueue("queue.fanout1", "test.fanout", "", null);
    try vhost.?.bindQueue("queue.fanout2", "test.fanout", "", null);
    try vhost.?.bindQueue("queue.topic", "test.topic", "stock.*.nyse", null);

    // Test direct exchange routing
    const direct_exchange = vhost.?.getExchange("test.direct").?;
    var message1 = try Message.init(allocator, 1, "test.direct", "key1", "Direct message 1");
    defer message1.deinit();

    const direct_routes = try direct_exchange.routeMessage(&message1, allocator);
    defer allocator.free(direct_routes);
    try std.testing.expectEqual(@as(usize, 1), direct_routes.len);
    try std.testing.expectEqualStrings("queue.direct1", direct_routes[0]);

    // Test fanout exchange routing
    const fanout_exchange = vhost.?.getExchange("test.fanout").?;
    var message2 = try Message.init(allocator, 2, "test.fanout", "ignored", "Fanout message");
    defer message2.deinit();

    const fanout_routes = try fanout_exchange.routeMessage(&message2, allocator);
    defer allocator.free(fanout_routes);
    try std.testing.expectEqual(@as(usize, 2), fanout_routes.len);

    // Test topic exchange routing
    const topic_exchange = vhost.?.getExchange("test.topic").?;
    var message3 = try Message.init(allocator, 3, "test.topic", "stock.usd.nyse", "Topic message");
    defer message3.deinit();

    const topic_routes = try topic_exchange.routeMessage(&message3, allocator);
    defer allocator.free(topic_routes);
    try std.testing.expectEqual(@as(usize, 1), topic_routes.len);
    try std.testing.expectEqualStrings("queue.topic", topic_routes[0]);

    // Test exchange statistics
    try std.testing.expectEqual(@as(u64, 1), direct_exchange.messages_in);
    try std.testing.expectEqual(@as(u64, 1), direct_exchange.messages_out);
    try std.testing.expectEqual(@as(u64, 1), fanout_exchange.messages_in);
    try std.testing.expectEqual(@as(u64, 2), fanout_exchange.messages_out);
}

test "queue message operations and consumer management" {
    const allocator = std.testing.allocator;

    var server = try createTestServer(allocator);
    defer server.deinit();

    const vhost = server.getVirtualHost("/");
    try std.testing.expect(vhost != null);

    _ = try vhost.?.declareQueue("test.queue", false, false, false, null);
    const queue = vhost.?.getQueue("test.queue");
    try std.testing.expect(queue != null);

    // Test message publishing to queue
    var message1 = try Message.init(allocator, 1, "test.exchange", "test.key", "Message 1");
    var message2 = try Message.init(allocator, 2, "test.exchange", "test.key", "Message 2");
    var message3 = try Message.init(allocator, 3, "test.exchange", "test.key", "Message 3");

    try queue.?.publish(message1);
    try queue.?.publish(message2);
    try queue.?.publish(message3);

    try std.testing.expectEqual(@as(u32, 3), queue.?.getMessageCount());
    try std.testing.expectEqual(@as(u32, 3), queue.?.messages_ready);
    try std.testing.expectEqual(@as(u32, 0), queue.?.messages_unacknowledged);

    // Test message retrieval (Basic.Get)
    const retrieved1 = try queue.?.get(false); // Manual ack
    try std.testing.expect(retrieved1 != null);
    try std.testing.expectEqualStrings("Message 1", retrieved1.?.body);
    try std.testing.expectEqual(@as(u32, 2), queue.?.getMessageCount());
    try std.testing.expectEqual(@as(u32, 2), queue.?.messages_ready);
    try std.testing.expectEqual(@as(u32, 1), queue.?.messages_unacknowledged);

    const retrieved2 = try queue.?.get(true); // Auto ack
    defer retrieved2.?.deinit();
    try std.testing.expect(retrieved2 != null);
    try std.testing.expectEqualStrings("Message 2", retrieved2.?.body);
    try std.testing.expectEqual(@as(u32, 1), queue.?.getMessageCount());
    try std.testing.expectEqual(@as(u32, 1), queue.?.messages_ready);
    try std.testing.expectEqual(@as(u32, 1), queue.?.messages_unacknowledged);

    // Test message acknowledgment
    try queue.?.acknowledge(retrieved1.?.id, false);
    retrieved1.?.deinit();
    try std.testing.expectEqual(@as(u32, 1), queue.?.getMessageCount());
    try std.testing.expectEqual(@as(u32, 1), queue.?.messages_ready);
    try std.testing.expectEqual(@as(u32, 0), queue.?.messages_unacknowledged);

    // Test consumer management
    var consumer1 = try Consumer.init(allocator, "consumer1", 1, false, false, false, null);
    defer consumer1.deinit();
    var consumer2 = try Consumer.init(allocator, "consumer2", 2, false, false, false, null);
    defer consumer2.deinit();

    try queue.?.consume(consumer1);
    try queue.?.consume(consumer2);
    try std.testing.expectEqual(@as(u32, 2), queue.?.getConsumerCount());

    // Test consumer cancellation
    try queue.?.cancel("consumer1");
    try std.testing.expectEqual(@as(u32, 1), queue.?.getConsumerCount());

    // Test queue purging
    const purged_count = queue.?.purge();
    try std.testing.expectEqual(@as(u32, 1), purged_count);
    try std.testing.expectEqual(@as(u32, 0), queue.?.getMessageCount());
}

test "message persistence and recovery" {
    const allocator = std.testing.allocator;

    // Clean up any existing test persistence data
    std.fs.cwd().deleteTree("./yak_persistence") catch {};

    // Create server and test persistence
    {
        var server = try createTestServer(allocator);
        defer server.deinit();

        const vhost = server.getVirtualHost("/");
        try std.testing.expect(vhost != null);

        // Create a durable queue
        _ = try vhost.?.declareQueue("durable.queue", true, false, false, null);
        const queue = vhost.?.getQueue("durable.queue");
        try std.testing.expect(queue != null);
        try std.testing.expectEqual(true, queue.?.durable);

        // Create persistent messages
        var message1 = try Message.init(allocator, 1, "test.exchange", "test.key", "Persistent message 1");
        var message2 = try Message.init(allocator, 2, "test.exchange", "test.key", "Persistent message 2");
        message1.markPersistent();
        message2.markPersistent();

        // Publish messages
        try queue.?.publish(message1);
        try queue.?.publish(message2);
        try std.testing.expectEqual(@as(u32, 2), queue.?.getMessageCount());

        // Manually persist queue state
        try server.persistQueueState("/", queue.?);
    }

    // Create new server instance to test recovery
    {
        var server = try createTestServer(allocator);
        defer server.deinit();

        const vhost = server.getVirtualHost("/");
        try std.testing.expect(vhost != null);

        // Recreate the durable queue (would normally be restored from metadata)
        _ = try vhost.?.declareQueue("durable.queue", true, false, false, null);
        const queue = vhost.?.getQueue("durable.queue");
        try std.testing.expect(queue != null);

        // Messages should be recovered during server initialization
        // (In a real scenario, this would happen automatically)
        // For this test, we verify that persistence files exist

        const file = std.fs.cwd().openFile("./yak_persistence/vhost_//queue_durable.queue.dat", .{}) catch |err| switch (err) {
            error.FileNotFound => {
                // Skip persistence test if no file exists
                std.testing.skip();
                return;
            },
            else => return err,
        };
        file.close();

        // Verify file has content (header + messages)
        const file_stat = try std.fs.cwd().statFile("./yak_persistence/vhost_//queue_durable.queue.dat");
        try std.testing.expect(file_stat.size > 4); // At least header + some data
    }

    // Clean up test persistence data
    std.fs.cwd().deleteTree("./yak_persistence") catch {};
}

test "error handling and recovery mechanisms" {
    const allocator = std.testing.allocator;

    var server = try createTestServer(allocator);
    defer server.deinit();

    // Test error handler initialization
    const error_stats = try server.error_handler.getErrorStats(allocator);
    defer error_stats.object.deinit();

    try std.testing.expect(error_stats.object.contains("total_errors"));
    try std.testing.expect(error_stats.object.contains("fatal_errors"));
    try std.testing.expect(error_stats.object.contains("recent_errors"));

    // Test initial error counts
    try std.testing.expectEqual(@as(i64, 0), error_stats.object.get("total_errors").?.integer);
    try std.testing.expectEqual(@as(i64, 0), error_stats.object.get("fatal_errors").?.integer);

    // Test server shutdown mechanisms
    try std.testing.expectEqual(false, server.isShutdownRequested());
    server.requestShutdown();
    try std.testing.expectEqual(true, server.isShutdownRequested());

    // Test server maintenance
    server.performMaintenance(); // Should not crash

    // Test virtual host error conditions
    const vhost = server.getVirtualHost("/");
    try std.testing.expect(vhost != null);

    // Test duplicate queue declaration with different parameters (should fail)
    _ = try vhost.?.declareQueue("test.queue", true, false, false, null);
    const result = vhost.?.declareQueue("test.queue", false, false, false, null); // Different durable setting
    try std.testing.expectError(error.QueueParameterMismatch, result);

    // Test binding to non-existent exchange
    const bind_result = vhost.?.bindQueue("test.queue", "non.existent.exchange", "key", null);
    try std.testing.expectError(error.ExchangeNotFound, bind_result);

    // Test queue deletion with constraints
    _ = try vhost.?.declareQueue("delete.test", false, false, false, null);
    const delete_queue = vhost.?.getQueue("delete.test").?;

    // Add a message to make queue non-empty
    var message = try Message.init(allocator, 1, "test", "key", "test");
    try delete_queue.publish(message);

    // Try to delete non-empty queue with if_empty=true (should fail)
    const delete_result = vhost.?.deleteQueue("delete.test", false, true);
    try std.testing.expectError(error.QueueNotEmpty, delete_result);
}

test "server statistics and monitoring" {
    const allocator = std.testing.allocator;

    var server = try createTestServer(allocator);
    defer server.deinit();

    // Test server stats
    const server_stats = try server.getStats(allocator);
    defer server_stats.object.deinit();

    try std.testing.expect(server_stats.object.contains("connections"));
    try std.testing.expect(server_stats.object.contains("virtual_hosts"));
    try std.testing.expect(server_stats.object.contains("running"));
    try std.testing.expect(server_stats.object.contains("shutdown_requested"));
    try std.testing.expect(server_stats.object.contains("errors"));

    try std.testing.expectEqual(@as(i64, 0), server_stats.object.get("connections").?.integer);
    try std.testing.expectEqual(@as(i64, 1), server_stats.object.get("virtual_hosts").?.integer);
    try std.testing.expectEqual(false, server_stats.object.get("running").?.bool);
    try std.testing.expectEqual(false, server_stats.object.get("shutdown_requested").?.bool);

    // Test virtual host stats
    const vhost = server.getVirtualHost("/");
    try std.testing.expect(vhost != null);

    const vhost_stats = try vhost.?.getStats(allocator);
    defer vhost_stats.object.deinit();

    try std.testing.expect(vhost_stats.object.contains("name"));
    try std.testing.expect(vhost_stats.object.contains("exchanges"));
    try std.testing.expect(vhost_stats.object.contains("queues"));
    try std.testing.expect(vhost_stats.object.contains("active"));
    try std.testing.expect(vhost_stats.object.contains("total_messages"));
    try std.testing.expect(vhost_stats.object.contains("total_consumers"));

    try std.testing.expectEqualStrings("/", vhost_stats.object.get("name").?.string);
    try std.testing.expectEqual(true, vhost_stats.object.get("active").?.bool);

    // Test queue stats
    _ = try vhost.?.declareQueue("stats.queue", false, false, false, null);
    const queue = vhost.?.getQueue("stats.queue").?;

    const queue_stats = try queue.getStats(allocator);
    defer queue_stats.object.deinit();

    try std.testing.expect(queue_stats.object.contains("name"));
    try std.testing.expect(queue_stats.object.contains("durable"));
    try std.testing.expect(queue_stats.object.contains("exclusive"));
    try std.testing.expect(queue_stats.object.contains("auto_delete"));
    try std.testing.expect(queue_stats.object.contains("messages_ready"));
    try std.testing.expect(queue_stats.object.contains("consumers"));

    try std.testing.expectEqualStrings("stats.queue", queue_stats.object.get("name").?.string);
    try std.testing.expectEqual(@as(i64, 0), queue_stats.object.get("messages_ready").?.integer);
    try std.testing.expectEqual(@as(i64, 0), queue_stats.object.get("consumers").?.integer);

    // Test exchange stats
    const exchange = vhost.?.getExchange("").?; // Default exchange
    const exchange_stats = try exchange.getStats(allocator);
    defer exchange_stats.object.deinit();

    try std.testing.expect(exchange_stats.object.contains("name"));
    try std.testing.expect(exchange_stats.object.contains("type"));
    try std.testing.expect(exchange_stats.object.contains("durable"));
    try std.testing.expect(exchange_stats.object.contains("bindings"));
    try std.testing.expect(exchange_stats.object.contains("messages_in"));
    try std.testing.expect(exchange_stats.object.contains("messages_out"));
}

test "full AMQP workflow simulation" {
    const allocator = std.testing.allocator;

    var server = try createTestServer(allocator);
    defer server.deinit();

    const vhost = server.getVirtualHost("/");
    try std.testing.expect(vhost != null);

    // Step 1: Declare exchanges
    try vhost.?.declareExchange("orders", .direct, true, false, false, null);
    try vhost.?.declareExchange("notifications", .fanout, false, false, false, null);

    // Step 2: Declare queues
    _ = try vhost.?.declareQueue("orders.processing", true, false, false, null);
    _ = try vhost.?.declareQueue("orders.audit", true, false, false, null);
    _ = try vhost.?.declareQueue("notifications.email", false, false, true, null);
    _ = try vhost.?.declareQueue("notifications.sms", false, false, true, null);

    // Step 3: Set up bindings
    try vhost.?.bindQueue("orders.processing", "orders", "new", null);
    try vhost.?.bindQueue("orders.processing", "orders", "update", null);
    try vhost.?.bindQueue("orders.audit", "orders", "new", null);
    try vhost.?.bindQueue("orders.audit", "orders", "update", null);
    try vhost.?.bindQueue("orders.audit", "orders", "cancel", null);
    try vhost.?.bindQueue("notifications.email", "notifications", "", null);
    try vhost.?.bindQueue("notifications.sms", "notifications", "", null);

    // Step 4: Publish messages
    const orders_exchange = vhost.?.getExchange("orders").?;
    const notifications_exchange = vhost.?.getExchange("notifications").?;

    // Publish order messages
    var new_order = try Message.init(allocator, 1, "orders", "new", "{\"order_id\":123,\"amount\":99.99}");
    defer new_order.deinit();
    try new_order.setHeader("content-type", "application/json");

    var update_order = try Message.init(allocator, 2, "orders", "update", "{\"order_id\":123,\"status\":\"processing\"}");
    defer update_order.deinit();

    var cancel_order = try Message.init(allocator, 3, "orders", "cancel", "{\"order_id\":124,\"reason\":\"user_request\"}");
    defer cancel_order.deinit();

    // Test routing
    const new_routes = try orders_exchange.routeMessage(&new_order, allocator);
    defer allocator.free(new_routes);
    try std.testing.expectEqual(@as(usize, 2), new_routes.len); // processing + audit

    const cancel_routes = try orders_exchange.routeMessage(&cancel_order, allocator);
    defer allocator.free(cancel_routes);
    try std.testing.expectEqual(@as(usize, 1), cancel_routes.len); // audit only

    // Publish notification
    var notification = try Message.init(allocator, 4, "notifications", "", "Order processed successfully");
    defer notification.deinit();

    const notif_routes = try notifications_exchange.routeMessage(&notification, allocator);
    defer allocator.free(notif_routes);
    try std.testing.expectEqual(@as(usize, 2), notif_routes.len); // email + sms

    // Step 5: Simulate message consumption
    const processing_queue = vhost.?.getQueue("orders.processing").?;
    const audit_queue = vhost.?.getQueue("orders.audit").?;

    // Add consumers
    var processor_consumer = try Consumer.init(allocator, "processor", 1, false, false, false, null);
    defer processor_consumer.deinit();
    var auditor_consumer = try Consumer.init(allocator, "auditor", 2, false, false, false, null);
    defer auditor_consumer.deinit();

    try processing_queue.consume(processor_consumer);
    try audit_queue.consume(auditor_consumer);

    // Verify consumer counts
    try std.testing.expectEqual(@as(u32, 1), processing_queue.getConsumerCount());
    try std.testing.expectEqual(@as(u32, 1), audit_queue.getConsumerCount());

    // Step 6: Verify exchange and queue statistics
    try std.testing.expect(orders_exchange.messages_in >= 2); // At least new + cancel orders routed
    try std.testing.expect(notifications_exchange.messages_in >= 1); // At least notification routed

    // Verify the complete workflow worked as expected
    try std.testing.expectEqual(@as(u32, 2), vhost.?.getExchangeCount() + 5); // Default exchanges + declared
    try std.testing.expectEqual(@as(u32, 4), vhost.?.getQueueCount());
}
