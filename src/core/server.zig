const std = @import("std");
const Config = @import("../config.zig").Config;
const VirtualHost = @import("vhost.zig").VirtualHost;
const NetworkConnection = @import("../network/connection.zig").Connection;
const ProtocolHandler = @import("../network/protocol.zig").ProtocolHandler;
const Message = @import("../message.zig").Message;
const Queue = @import("../routing/queue.zig").Queue;
const Exchange = @import("../routing/exchange.zig").Exchange;
const ErrorHandler = @import("../error/error_handler.zig").ErrorHandler;
const ErrorHelpers = @import("../error/error_handler.zig").ErrorHelpers;
const RecoveryAction = @import("../error/error_handler.zig").RecoveryAction;
const MetricsRegistry = @import("../metrics/metrics.zig").MetricsRegistry;
const MetricsHttpServer = @import("../metrics/http_server.zig").MetricsHttpServer;
const BrokerMetricsCollector = @import("../metrics/broker_collector.zig").BrokerMetricsCollector;
const SslContext = @import("../network/ssl.zig").SslContext;
const SslListener = @import("../network/ssl.zig").SslListener;
const Cluster = @import("../cluster/cluster.zig").Cluster;

// Context structure for CLI client thread
const CliClientContext = struct {
    server: *Server,
    connection: std.net.Server.Connection,
    client_id: u64,
};

// Global reference to server instance for persistence callback
var global_server: ?*Server = null;

fn serverPersistMessage(vhost_name: []const u8, queue_name: []const u8, message: *const Message) !void {
    if (global_server) |server| {
        try server.persistMessage(vhost_name, queue_name, message);
    }
}

fn serverHandleError(error_info: @import("../error/error_handler.zig").ErrorInfo) RecoveryAction {
    if (global_server) |server| {
        return server.error_handler.handleError(error_info);
    }
    return .close_connection; // Default fallback
}

pub const Server = struct {
    allocator: std.mem.Allocator,
    config: Config,
    vhosts: std.HashMap([]const u8, *VirtualHost, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    connections: std.ArrayList(*NetworkConnection),
    next_connection_id: u64,
    next_cli_client_id: std.atomic.Value(u64),
    running: bool,
    tcp_server: ?std.net.Server,
    ssl_context: ?SslContext,
    ssl_listener: ?SslListener,
    ssl_thread: ?std.Thread,
    cli_server: ?std.net.Server,
    cli_thread: ?std.Thread,
    protocol_handler: ProtocolHandler,
    persistence_mutex: std.Thread.Mutex,
    error_handler: ErrorHandler,
    shutdown_requested: std.atomic.Value(bool),

    // Metrics components
    metrics_registry: ?MetricsRegistry,
    metrics_http_server: ?MetricsHttpServer,
    metrics_collector: ?BrokerMetricsCollector,

    // Cluster components
    cluster: ?Cluster,

    pub fn init(allocator: std.mem.Allocator, config: Config) !Server {
        const protocol_handler = ProtocolHandler.init(allocator);

        var server = Server{
            .allocator = allocator,
            .config = config,
            .vhosts = std.HashMap([]const u8, *VirtualHost, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .connections = std.ArrayList(*NetworkConnection).init(allocator),
            .next_connection_id = 1,
            .next_cli_client_id = std.atomic.Value(u64).init(1),
            .running = false,
            .tcp_server = null,
            .ssl_context = null,
            .ssl_listener = null,
            .ssl_thread = null,
            .cli_server = null,
            .cli_thread = null,
            .protocol_handler = protocol_handler,
            .persistence_mutex = std.Thread.Mutex{},
            .error_handler = ErrorHandler.init(allocator),
            .shutdown_requested = std.atomic.Value(bool).init(false),
            .metrics_registry = null,
            .metrics_http_server = null,
            .metrics_collector = null,
            .cluster = null,
        };

        // Set up global server reference for persistence callback
        global_server = &server;

        // Set up persistence callback in protocol handler
        server.protocol_handler.persist_message_fn = serverPersistMessage;

        // Set up error handler callback in protocol handler
        server.protocol_handler.error_handler_fn = serverHandleError;

        // Create default virtual host
        try server.createVirtualHost("/");

        // Set server reference for default virtual host for cluster operations
        if (server.getVirtualHost("/")) |default_vhost| {
            default_vhost.setServerReference(&server);
        }

        // Initialize SSL context if enabled
        if (server.config.ssl.enabled) {
            try server.initializeSsl();
        }

        // Initialize metrics if enabled
        if (server.config.metrics.enabled) {
            try server.initializeMetrics();
        }

        // Initialize cluster if enabled
        if (server.config.cluster.enabled) {
            try server.initializeCluster();
        }

        // Recover persistent state from storage
        server.recoverPersistentState() catch |err| {
            const error_info = ErrorHelpers.recoverableError(.resource_error, "Failed to recover persistent state from storage");
            const action = server.error_handler.handleError(error_info);

            if (action == .shutdown_server) {
                return err;
            }
        };

        // Note: Virtual host integration will be handled within the protocol handler
        // using the connection's setVirtualHost method

        return server;
    }

    pub fn deinit(self: *Server) void {
        // Clear global reference
        global_server = null;

        self.stop();

        // Wait for CLI thread to finish if it's running
        if (self.cli_thread) |thread| {
            thread.join();
        }

        // Wait for SSL thread to finish if it's running
        if (self.ssl_thread) |thread| {
            thread.join();
        }

        // Clean up SSL components
        if (self.ssl_listener) |*ssl_listener| {
            ssl_listener.deinit();
        }
        if (self.ssl_context) |*ssl_context| {
            ssl_context.deinit();
        }

        // Clean up cluster components
        if (self.cluster) |*cluster| {
            cluster.deinit();
        }

        // Clean up metrics components
        if (self.metrics_collector) |*collector| {
            collector.deinit();
        }
        if (self.metrics_http_server) |*http_server| {
            http_server.deinit();
        }
        if (self.metrics_registry) |*registry| {
            registry.deinit();
        }

        // Clean up error handler
        self.error_handler.deinit();

        // Clean up protocol handler
        self.protocol_handler.deinit();

        // Clean up connections
        for (self.connections.items) |connection| {
            connection.deinit();
            self.allocator.destroy(connection);
        }
        self.connections.deinit();

        // Clean up virtual hosts
        var vhost_iterator = self.vhosts.iterator();
        while (vhost_iterator.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.vhosts.deinit();
    }

    pub fn start(self: *Server) !void {
        std.log.info("Starting Yak AMQP server on {s}:{d}", .{ self.config.tcp.host, self.config.tcp.port });

        const address = try std.net.Address.parseIp(self.config.tcp.host, self.config.tcp.port);
        self.tcp_server = try address.listen(.{
            .reuse_address = true,
        });

        self.running = true;

        // Start cluster if enabled
        if (self.cluster) |*cluster| {
            const default_vhost = self.getVirtualHost("/").?;
            try cluster.start(default_vhost);
        }

        // Start accepting connections
        while (self.running and !self.shutdown_requested.load(.monotonic)) {
            const client_socket = self.tcp_server.?.accept() catch {
                const error_info = ErrorHelpers.recoverableError(.resource_error, "Failed to accept incoming connection");
                const action = self.error_handler.handleError(error_info);

                switch (action) {
                    .shutdown_server => {
                        std.log.err("Shutting down server due to connection accept failures", .{});
                        self.requestShutdown();
                        break;
                    },
                    else => {
                        // Brief pause before retrying to avoid tight error loop
                        std.Thread.sleep(100 * std.time.ns_per_ms);
                        continue;
                    },
                }
            };

            // Handle connection with comprehensive error recovery
            self.handleConnectionWithRecovery(client_socket);
        }
    }

    pub fn startWithShutdownMonitoring(self: *Server, shutdown_flag: *const bool) !void {
        std.log.info("Starting Yak AMQP server on {s}:{d}", .{ self.config.tcp.host, self.config.tcp.port });

        const address = try std.net.Address.parseIp(self.config.tcp.host, self.config.tcp.port);
        self.tcp_server = try address.listen(.{
            .reuse_address = true,
        });

        self.running = true;

        // Start accepting connections with shutdown monitoring
        while (self.running and !shutdown_flag.* and !self.shutdown_requested.load(.monotonic)) {
            // Use accept with timeout to periodically check shutdown flag
            const client_socket = self.acceptWithTimeout(100) catch |err| switch (err) {
                error.WouldBlock => continue, // Timeout, check shutdown flag
                else => {
                    const error_info = ErrorHelpers.recoverableError(.resource_error, "Failed to accept connection with timeout");
                    const action = self.error_handler.handleError(error_info);

                    switch (action) {
                        .shutdown_server => {
                            std.log.err("Shutting down server due to persistent connection failures", .{});
                            self.requestShutdown();
                            break;
                        },
                        else => {
                            std.Thread.sleep(100 * std.time.ns_per_ms);
                            continue;
                        },
                    }
                },
            };

            if (client_socket) |socket| {
                // Handle connection with comprehensive error recovery
                self.handleConnectionWithRecovery(socket);
            }
        }

        if (shutdown_flag.*) {
            std.log.info("Shutdown signal received, stopping server gracefully", .{});
            self.stop();
        }
    }

    fn acceptWithTimeout(self: *Server, timeout_ms: u32) !?std.net.Server.Connection {
        _ = timeout_ms;

        // Use poll to check if socket is ready with timeout
        var pollfds = [_]std.posix.pollfd{
            std.posix.pollfd{
                .fd = self.tcp_server.?.stream.handle,
                .events = std.posix.POLL.IN,
                .revents = 0,
            },
        };

        const poll_result = try std.posix.poll(&pollfds, 100); // 100ms timeout
        if (poll_result == 0) {
            return null; // Timeout
        }

        return try self.tcp_server.?.accept();
    }

    pub fn stop(self: *Server) void {
        self.running = false;

        // Stop cluster if enabled
        if (self.cluster) |*cluster| {
            cluster.stop();
        }

        if (self.tcp_server) |*server| {
            server.deinit();
            self.tcp_server = null;
        }
        if (self.cli_server) |*server| {
            server.deinit();
            self.cli_server = null;
        }
    }

    pub fn startCliServer(self: *Server) !void {
        if (!self.config.cli.enabled) return;

        std.log.info("Starting CLI server on {s}", .{self.config.cli.socket_path});

        // Remove existing socket file if it exists
        std.fs.cwd().deleteFile(self.config.cli.socket_path) catch {};

        const address = try std.net.Address.initUnix(self.config.cli.socket_path);
        self.cli_server = try address.listen(.{});

        // Start CLI server in separate thread
        self.cli_thread = try std.Thread.spawn(.{}, cliServerThread, .{self});

        std.log.info("CLI server thread started successfully", .{});
    }

    fn cliServerThread(self: *Server) void {
        if (self.cli_server == null) {
            std.log.err("CLI server not initialized when starting thread", .{});
            return;
        }

        // Main CLI server loop
        while (self.running and !self.shutdown_requested.load(.monotonic)) {
            // Accept CLI client connections with timeout to check shutdown status
            const client_connection = self.acceptCliConnectionWithTimeout(100) catch |err| switch (err) {
                error.WouldBlock => continue, // Timeout, check shutdown flag
                else => {
                    const error_info = ErrorHelpers.recoverableError(.resource_error, "Failed to accept CLI connection");
                    const action = self.error_handler.handleError(error_info);

                    switch (action) {
                        .shutdown_server => {
                            std.log.err("Shutting down CLI server due to persistent connection failures", .{});
                            self.requestShutdown();
                            break;
                        },
                        else => {
                            std.Thread.sleep(100 * std.time.ns_per_ms);
                            continue;
                        },
                    }
                },
            };

            if (client_connection) |connection| {
                // Spawn a separate thread to handle each CLI client connection
                const client_id = self.next_cli_client_id.fetchAdd(1, .monotonic);

                // Allocate context for the client thread
                const context = self.allocator.create(CliClientContext) catch |err| {
                    std.log.err("Failed to allocate memory for CLI client context: {}", .{err});
                    connection.stream.close();
                    continue;
                };

                context.* = CliClientContext{
                    .server = self,
                    .connection = connection,
                    .client_id = client_id,
                };

                // Spawn thread to handle this client
                const thread = std.Thread.spawn(.{}, handleCliClientThread, .{context}) catch |err| {
                    std.log.err("Failed to spawn CLI client thread: {}", .{err});
                    self.allocator.destroy(context);
                    connection.stream.close();
                    continue;
                };

                // Detach the thread so it can clean up itself
                thread.detach();
            }
        }
    }

    fn acceptCliConnectionWithTimeout(self: *Server, timeout_ms: u32) !?std.net.Server.Connection {
        _ = timeout_ms;

        if (self.cli_server == null) {
            return error.ServerNotInitialized;
        }

        // Use poll to check if socket is ready with timeout
        var pollfds = [_]std.posix.pollfd{
            std.posix.pollfd{
                .fd = self.cli_server.?.stream.handle,
                .events = std.posix.POLL.IN,
                .revents = 0,
            },
        };

        const poll_result = try std.posix.poll(&pollfds, 100); // 100ms timeout
        if (poll_result == 0) {
            return null; // Timeout
        }

        return try self.cli_server.?.accept();
    }

    fn handleCliConnection(self: *Server, connection: std.net.Server.Connection) !void {

        // TODO: For now, send a simple welcome message and close
        // This will be expanded with actual CLI protocol handling
        const welcome_msg = "Welcome to Yak AMQP Broker CLI\nType 'help' for available commands\n> ";
        try connection.stream.writeAll(welcome_msg);

        // Read one command (basic implementation)
        var buffer: [1024]u8 = undefined;
        const bytes_read = try connection.stream.read(&buffer);

        if (bytes_read > 0) {
            const command = std.mem.trim(u8, buffer[0..bytes_read], " \n\r\t");
            std.log.info("CLI command received: {s}", .{command});

            // Basic command handling
            if (std.mem.eql(u8, command, "help")) {
                const help_msg = "Available commands:\n  help - Show this help\n  status - Show server status\n  metrics - Show server metrics\n  cluster status - Show cluster status and node information\n  list queues [vhost] - List all queues in virtual host (default: /)\n  list exchanges [vhost] - List all exchanges in virtual host (default: /)\n  list connections - List all active client connections\n  list vhosts - List all virtual hosts\n  queue info <name> [vhost] - Show detailed information about a queue\n  queue declare <name> [--durable] [--exclusive] [--auto-delete] [vhost] - Create a new queue\n  queue delete <name> [--if-unused] [--if-empty] [vhost] - Delete a queue\n  queue purge <name> [vhost] - Remove all messages from a queue\n  exchange declare <name> <type> [--durable] [--auto-delete] [vhost] - Create a new exchange\n  exchange delete <name> [--if-unused] [vhost] - Delete an exchange\n  vhost create <name> - Create a new virtual host\n  vhost delete <name> - Delete a virtual host\n  vhost info <name> - Show detailed information about a virtual host\n  exit - Close connection\n";
                try connection.stream.writeAll(help_msg);
            } else if (std.mem.startsWith(u8, command, "list queues")) {
                try self.handleListQueuesCommand(connection, command);
            } else if (std.mem.startsWith(u8, command, "queue info")) {
                try self.handleQueueInfoCommand(connection, command);
            } else if (std.mem.startsWith(u8, command, "queue declare")) {
                try self.handleQueueDeclareCommand(connection, command);
            } else if (std.mem.startsWith(u8, command, "queue delete")) {
                try self.handleQueueDeleteCommand(connection, command);
            } else if (std.mem.startsWith(u8, command, "queue purge")) {
                try self.handleQueuePurgeCommand(connection, command);
            } else if (std.mem.startsWith(u8, command, "exchange declare")) {
                try self.handleExchangeDeclareCommand(connection, command);
            } else if (std.mem.startsWith(u8, command, "exchange delete")) {
                try self.handleExchangeDeleteCommand(connection, command);
            } else if (std.mem.startsWith(u8, command, "list exchanges")) {
                try self.handleListExchangesCommand(connection, command);
            } else if (std.mem.startsWith(u8, command, "list connections")) {
                try self.handleListConnectionsCommand(connection, command);
            } else if (std.mem.startsWith(u8, command, "list vhosts")) {
                try self.handleListVHostsCommand(connection, command);
            } else if (std.mem.startsWith(u8, command, "vhost create")) {
                try self.handleVHostCreateCommand(connection, command);
            } else if (std.mem.startsWith(u8, command, "vhost delete")) {
                try self.handleVHostDeleteCommand(connection, command);
            } else if (std.mem.startsWith(u8, command, "vhost info")) {
                try self.handleVHostInfoCommand(connection, command);
            } else if (std.mem.eql(u8, command, "metrics")) {
                try self.handleMetricsCommand(connection, command);
            } else if (std.mem.startsWith(u8, command, "cluster status")) {
                try self.handleClusterStatusCommand(connection, command);
            } else if (std.mem.eql(u8, command, "status")) {
                const status_msg = try std.fmt.allocPrint(self.allocator, "Server Status:\n  Running: {}\n  Connections: {}\n  Virtual Hosts: {}\n  Shutdown Requested: {}\n", .{
                    self.running,
                    self.getConnectionCount(),
                    self.getVirtualHostCount(),
                    self.isShutdownRequested(),
                });
                defer self.allocator.free(status_msg);
                try connection.stream.writeAll(status_msg);
            } else if (std.mem.eql(u8, command, "exit")) {
                try connection.stream.writeAll("Goodbye!\n");
            } else {
                const error_msg = try std.fmt.allocPrint(self.allocator, "Unknown command: {s}\nType 'help' for available commands\n", .{command});
                defer self.allocator.free(error_msg);
                try connection.stream.writeAll(error_msg);
            }
        }
    }

    fn handleCliClientThread(context: *CliClientContext) void {
        defer context.server.allocator.destroy(context);
        defer context.connection.stream.close();

        std.log.info("CLI client {} connected on dedicated thread", .{context.client_id});

        // Handle the CLI client connection
        context.server.handleCliConnection(context.connection) catch |err| {
            std.log.warn("CLI client {} error: {}", .{ context.client_id, err });
        };

        std.log.info("CLI client {} thread terminating", .{context.client_id});
    }

    fn handleListQueuesCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        // Parse command to extract virtual host name
        var vhost_name: []const u8 = "/"; // Default virtual host

        // Split command by spaces to check for vhost parameter
        var parts = std.mem.splitSequence(u8, command, " ");
        _ = parts.next(); // "list"
        _ = parts.next(); // "queues"

        if (parts.next()) |vhost_arg| {
            vhost_name = std.mem.trim(u8, vhost_arg, " \t\n\r");
        }

        // Get the virtual host
        const vhost = self.getVirtualHost(vhost_name);
        if (vhost == null) {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Virtual host '{s}' not found\n", .{vhost_name});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        }

        // Get list of queues
        const queue_names = vhost.?.listQueues(self.allocator) catch |err| {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error listing queues: {}\n", .{err});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        };
        defer {
            for (queue_names) |name| {
                self.allocator.free(name);
            }
            self.allocator.free(queue_names);
        }

        // Format and send response
        if (queue_names.len == 0) {
            const no_queues_msg = try std.fmt.allocPrint(self.allocator, "No queues found in virtual host '{s}'\n", .{vhost_name});
            defer self.allocator.free(no_queues_msg);
            try connection.stream.writeAll(no_queues_msg);
        } else {
            const header_msg = try std.fmt.allocPrint(self.allocator, "Queues in virtual host '{s}':\n", .{vhost_name});
            defer self.allocator.free(header_msg);
            try connection.stream.writeAll(header_msg);

            for (queue_names) |queue_name| {
                // Get queue details for additional info
                const queue = vhost.?.getQueue(queue_name).?;
                const queue_info = try std.fmt.allocPrint(self.allocator, "  {s} (messages: {}, consumers: {}, durable: {})\n", .{
                    queue_name,
                    queue.getMessageCount(),
                    queue.getConsumerCount(),
                    queue.durable,
                });
                defer self.allocator.free(queue_info);
                try connection.stream.writeAll(queue_info);
            }
        }
    }

    fn handleQueueInfoCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        // Parse command to extract queue name and optional virtual host name
        var parts = std.mem.splitSequence(u8, command, " ");
        _ = parts.next(); // "queue"
        _ = parts.next(); // "info"

        const queue_name = parts.next() orelse {
            const error_msg = "Error: Queue name is required. Usage: queue info <name> [vhost]\n";
            try connection.stream.writeAll(error_msg);
            return;
        };

        var vhost_name: []const u8 = "/"; // Default virtual host
        if (parts.next()) |vhost_arg| {
            vhost_name = std.mem.trim(u8, vhost_arg, " \t\n\r");
        }

        // Get the virtual host
        const vhost = self.getVirtualHost(vhost_name);
        if (vhost == null) {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Virtual host '{s}' not found\n", .{vhost_name});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        }

        // Get the queue
        const queue = vhost.?.getQueue(queue_name);
        if (queue == null) {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Queue '{s}' not found in virtual host '{s}'\n", .{ queue_name, vhost_name });
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        }

        // Format and send detailed queue information
        const queue_info = try std.fmt.allocPrint(self.allocator,
            \\Queue Information: {s}
            \\  Virtual Host: {s}
            \\  Durable: {}
            \\  Exclusive: {}
            \\  Auto Delete: {}
            \\  Messages: {}
            \\  Consumers: {}
            \\  Bindings: {}
            \\  Memory Usage: {} bytes (estimated)
            \\
        , .{
            queue_name,
            vhost_name,
            queue.?.durable,
            queue.?.exclusive,
            queue.?.auto_delete,
            queue.?.getMessageCount(),
            queue.?.getConsumerCount(),
            queue.?.bindings.items.len,
            queue.?.memory_usage,
        });
        defer self.allocator.free(queue_info);
        try connection.stream.writeAll(queue_info);

        // Show queue bindings if any exist
        if (queue.?.bindings.items.len > 0) {
            const bindings_header = "  Bindings:\n";
            try connection.stream.writeAll(bindings_header);

            for (queue.?.bindings.items) |binding| {
                const binding_info = try std.fmt.allocPrint(self.allocator, "    Exchange: {s}, Routing Key: {s}\n", .{ binding.exchange_name, binding.routing_key });
                defer self.allocator.free(binding_info);
                try connection.stream.writeAll(binding_info);
            }
        } else {
            const no_bindings = "  No bindings configured\n";
            try connection.stream.writeAll(no_bindings);
        }
    }

    fn handleQueueDeclareCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        // Parse command to extract queue name, flags, and optional virtual host name
        var parts = std.mem.splitSequence(u8, command, " ");
        _ = parts.next(); // "queue"
        _ = parts.next(); // "declare"

        const queue_name = parts.next() orelse {
            const error_msg = "Error: Queue name is required. Usage: queue declare <name> [--durable] [--exclusive] [--auto-delete] [vhost]\n";
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Parse optional flags and virtual host
        var durable = false;
        var exclusive = false;
        var auto_delete = false;
        var vhost_name: []const u8 = "/"; // Default virtual host

        while (parts.next()) |part| {
            const trimmed_part = std.mem.trim(u8, part, " \t\n\r");
            if (std.mem.eql(u8, trimmed_part, "--durable")) {
                durable = true;
            } else if (std.mem.eql(u8, trimmed_part, "--exclusive")) {
                exclusive = true;
            } else if (std.mem.eql(u8, trimmed_part, "--auto-delete")) {
                auto_delete = true;
            } else if (!std.mem.startsWith(u8, trimmed_part, "--")) {
                // Assume it's a virtual host name if it doesn't start with --
                vhost_name = trimmed_part;
            }
        }

        // Get the virtual host
        const vhost = self.getVirtualHost(vhost_name);
        if (vhost == null) {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Virtual host '{s}' not found\n", .{vhost_name});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        }

        // Declare the queue
        const actual_queue_name = vhost.?.declareQueue(queue_name, durable, exclusive, auto_delete, null) catch |err| {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error creating queue '{s}': {}\n", .{ queue_name, err });
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Send success response
        const success_msg = try std.fmt.allocPrint(self.allocator,
            \\Queue declared successfully: {s}
            \\  Virtual Host: {s}
            \\  Durable: {}
            \\  Exclusive: {}
            \\  Auto Delete: {}
            \\
        , .{
            actual_queue_name,
            vhost_name,
            durable,
            exclusive,
            auto_delete,
        });
        defer self.allocator.free(success_msg);
        try connection.stream.writeAll(success_msg);
    }

    fn handleQueueDeleteCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        // Parse command to extract queue name, flags, and optional virtual host name
        var parts = std.mem.splitSequence(u8, command, " ");
        _ = parts.next(); // "queue"
        _ = parts.next(); // "delete"

        const queue_name = parts.next() orelse {
            const error_msg = "Error: Queue name is required. Usage: queue delete <name> [--if-unused] [--if-empty] [vhost]\n";
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Parse optional flags and virtual host
        var if_unused = false;
        var if_empty = false;
        var vhost_name: []const u8 = "/"; // Default virtual host

        while (parts.next()) |part| {
            const trimmed_part = std.mem.trim(u8, part, " \t\n\r");
            if (std.mem.eql(u8, trimmed_part, "--if-unused")) {
                if_unused = true;
            } else if (std.mem.eql(u8, trimmed_part, "--if-empty")) {
                if_empty = true;
            } else if (!std.mem.startsWith(u8, trimmed_part, "--")) {
                // Assume it's a virtual host name if it doesn't start with --
                vhost_name = trimmed_part;
            }
        }

        // Get the virtual host
        const vhost = self.getVirtualHost(vhost_name);
        if (vhost == null) {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Virtual host '{s}' not found\n", .{vhost_name});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        }

        // Check if queue exists before attempting deletion
        const queue = vhost.?.getQueue(queue_name);
        if (queue == null) {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Queue '{s}' not found in virtual host '{s}'\n", .{ queue_name, vhost_name });
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        }

        // Delete the queue
        const message_count = vhost.?.deleteQueue(queue_name, if_unused, if_empty) catch |err| {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error deleting queue '{s}': {}\n", .{ queue_name, err });
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Send success response
        const success_msg = try std.fmt.allocPrint(self.allocator,
            \\Queue deleted successfully: {s}
            \\  Virtual Host: {s}
            \\  Messages deleted: {}
            \\
        , .{
            queue_name,
            vhost_name,
            message_count,
        });
        defer self.allocator.free(success_msg);
        try connection.stream.writeAll(success_msg);
    }

    fn handleQueuePurgeCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        // Parse command to extract queue name and optional virtual host name
        var parts = std.mem.splitSequence(u8, command, " ");
        _ = parts.next(); // "queue"
        _ = parts.next(); // "purge"

        const queue_name = parts.next() orelse {
            const error_msg = "Error: Queue name is required. Usage: queue purge <name> [vhost]\n";
            try connection.stream.writeAll(error_msg);
            return;
        };

        var vhost_name: []const u8 = "/"; // Default virtual host
        if (parts.next()) |vhost_arg| {
            vhost_name = std.mem.trim(u8, vhost_arg, " \t\n\r");
        }

        // Get the virtual host
        const vhost = self.getVirtualHost(vhost_name);
        if (vhost == null) {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Virtual host '{s}' not found\n", .{vhost_name});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        }

        // Check if queue exists before attempting purge
        const queue = vhost.?.getQueue(queue_name);
        if (queue == null) {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Queue '{s}' not found in virtual host '{s}'\n", .{ queue_name, vhost_name });
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        }

        // Purge the queue
        const message_count = vhost.?.purgeQueue(queue_name) catch |err| {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error purging queue '{s}': {}\n", .{ queue_name, err });
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Send success response
        const success_msg = try std.fmt.allocPrint(self.allocator,
            \\Queue purged successfully: {s}
            \\  Virtual Host: {s}
            \\  Messages purged: {}
            \\
        , .{
            queue_name,
            vhost_name,
            message_count,
        });
        defer self.allocator.free(success_msg);
        try connection.stream.writeAll(success_msg);
    }

    fn handleListExchangesCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        // Parse command to extract virtual host name
        var vhost_name: []const u8 = "/"; // Default virtual host

        // Split command by spaces to check for vhost parameter
        var parts = std.mem.splitSequence(u8, command, " ");
        _ = parts.next(); // "list"
        _ = parts.next(); // "exchanges"

        if (parts.next()) |vhost_arg| {
            vhost_name = std.mem.trim(u8, vhost_arg, " \t\n\r");
        }

        // Get the virtual host
        const vhost = self.getVirtualHost(vhost_name);
        if (vhost == null) {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Virtual host '{s}' not found\n", .{vhost_name});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        }

        // Get list of exchanges
        const exchange_names = vhost.?.listExchanges(self.allocator) catch |err| {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error listing exchanges: {}\n", .{err});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        };
        defer {
            for (exchange_names) |name| {
                self.allocator.free(name);
            }
            self.allocator.free(exchange_names);
        }

        // Format and send response
        if (exchange_names.len == 0) {
            const no_exchanges_msg = try std.fmt.allocPrint(self.allocator, "No exchanges found in virtual host '{s}'\n", .{vhost_name});
            defer self.allocator.free(no_exchanges_msg);
            try connection.stream.writeAll(no_exchanges_msg);
        } else {
            const header_msg = try std.fmt.allocPrint(self.allocator, "Exchanges in virtual host '{s}':\n", .{vhost_name});
            defer self.allocator.free(header_msg);
            try connection.stream.writeAll(header_msg);

            for (exchange_names) |exchange_name| {
                // Get exchange details for additional info
                const exchange = vhost.?.getExchange(exchange_name).?;
                const exchange_info = try std.fmt.allocPrint(self.allocator, "  {s} (type: {s}, durable: {}, bindings: {})\n", .{
                    exchange_name,
                    @tagName(exchange.exchange_type),
                    exchange.durable,
                    exchange.bindings.items.len,
                });
                defer self.allocator.free(exchange_info);
                try connection.stream.writeAll(exchange_info);
            }
        }
    }

    fn handleExchangeDeclareCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        // Parse command to extract exchange name, type, flags, and optional virtual host name
        var parts = std.mem.splitSequence(u8, command, " ");
        _ = parts.next(); // "exchange"
        _ = parts.next(); // "declare"

        const exchange_name = parts.next() orelse {
            const error_msg = "Error: Exchange name is required. Usage: exchange declare <name> <type> [--durable] [--auto-delete] [vhost]\n";
            try connection.stream.writeAll(error_msg);
            return;
        };

        const exchange_type_str = parts.next() orelse {
            const error_msg = "Error: Exchange type is required. Valid types: direct, fanout, topic, headers\n";
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Parse exchange type
        const exchange_type = if (std.mem.eql(u8, exchange_type_str, "direct"))
            @import("../routing/exchange.zig").ExchangeType.direct
        else if (std.mem.eql(u8, exchange_type_str, "fanout"))
            @import("../routing/exchange.zig").ExchangeType.fanout
        else if (std.mem.eql(u8, exchange_type_str, "topic"))
            @import("../routing/exchange.zig").ExchangeType.topic
        else if (std.mem.eql(u8, exchange_type_str, "headers"))
            @import("../routing/exchange.zig").ExchangeType.headers
        else {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Invalid exchange type '{s}'. Valid types: direct, fanout, topic, headers\n", .{exchange_type_str});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Parse optional flags and virtual host
        var durable = false;
        var auto_delete = false;
        var vhost_name: []const u8 = "/"; // Default virtual host

        while (parts.next()) |part| {
            const trimmed_part = std.mem.trim(u8, part, " \t\n\r");
            if (std.mem.eql(u8, trimmed_part, "--durable")) {
                durable = true;
            } else if (std.mem.eql(u8, trimmed_part, "--auto-delete")) {
                auto_delete = true;
            } else if (!std.mem.startsWith(u8, trimmed_part, "--")) {
                // Assume it's a virtual host name if it doesn't start with --
                vhost_name = trimmed_part;
            }
        }

        // Get the virtual host
        const vhost = self.getVirtualHost(vhost_name);
        if (vhost == null) {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Virtual host '{s}' not found\n", .{vhost_name});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        }

        // Declare the exchange
        vhost.?.declareExchange(exchange_name, exchange_type, durable, auto_delete, false, null) catch |err| {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error creating exchange '{s}': {}\n", .{ exchange_name, err });
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Send success response
        const success_msg = try std.fmt.allocPrint(self.allocator,
            \\Exchange declared successfully: {s}
            \\  Virtual Host: {s}
            \\  Type: {s}
            \\  Durable: {}
            \\  Auto Delete: {}
            \\
        , .{
            exchange_name,
            vhost_name,
            @tagName(exchange_type),
            durable,
            auto_delete,
        });
        defer self.allocator.free(success_msg);
        try connection.stream.writeAll(success_msg);
    }

    fn handleExchangeDeleteCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        // Parse command to extract exchange name, flags, and optional virtual host name
        var parts = std.mem.splitSequence(u8, command, " ");
        _ = parts.next(); // "exchange"
        _ = parts.next(); // "delete"

        const exchange_name = parts.next() orelse {
            const error_msg = "Error: Exchange name is required. Usage: exchange delete <name> [--if-unused] [vhost]\n";
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Parse optional flags and virtual host
        var if_unused = false;
        var vhost_name: []const u8 = "/"; // Default virtual host

        while (parts.next()) |part| {
            const trimmed_part = std.mem.trim(u8, part, " \t\n\r");
            if (std.mem.eql(u8, trimmed_part, "--if-unused")) {
                if_unused = true;
            } else if (!std.mem.startsWith(u8, trimmed_part, "--")) {
                // Assume it's a virtual host name if it doesn't start with --
                vhost_name = trimmed_part;
            }
        }

        // Get the virtual host
        const vhost = self.getVirtualHost(vhost_name);
        if (vhost == null) {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Virtual host '{s}' not found\n", .{vhost_name});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        }

        // Check if exchange exists before attempting deletion
        const exchange = vhost.?.getExchange(exchange_name);
        if (exchange == null) {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Exchange '{s}' not found in virtual host '{s}'\n", .{ exchange_name, vhost_name });
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        }

        // Delete the exchange
        vhost.?.deleteExchange(exchange_name, if_unused) catch |err| {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error deleting exchange '{s}': {}\n", .{ exchange_name, err });
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Send success response
        const success_msg = try std.fmt.allocPrint(self.allocator,
            \\Exchange deleted successfully: {s}
            \\  Virtual Host: {s}
            \\
        , .{
            exchange_name,
            vhost_name,
        });
        defer self.allocator.free(success_msg);
        try connection.stream.writeAll(success_msg);
    }

    fn handleListConnectionsCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        _ = command; // No additional parameters needed for listing connections

        // Format and send connection list
        if (self.connections.items.len == 0) {
            const no_connections_msg = "No active client connections\n";
            try connection.stream.writeAll(no_connections_msg);
        } else {
            const header_msg = try std.fmt.allocPrint(self.allocator, "Active client connections ({}):\n", .{self.connections.items.len});
            defer self.allocator.free(header_msg);
            try connection.stream.writeAll(header_msg);

            for (self.connections.items) |conn| {
                const vhost_name = conn.virtual_host orelse "none";
                const state_name = @tagName(conn.state);
                const channel_count = conn.channels.count();

                const connection_info = try std.fmt.allocPrint(self.allocator,
                    \\  Connection ID: {}
                    \\    State: {s}
                    \\    Virtual Host: {s}
                    \\    Authenticated: {}
                    \\    Channels: {}
                    \\    Heartbeat Interval: {}s
                    \\    Max Frame Size: {} bytes
                    \\    Blocked: {}
                    \\
                , .{
                    conn.id,
                    state_name,
                    vhost_name,
                    conn.authenticated,
                    channel_count,
                    conn.heartbeat_interval,
                    conn.max_frame_size,
                    conn.blocked,
                });
                defer self.allocator.free(connection_info);
                try connection.stream.writeAll(connection_info);
            }
        }
    }

    fn handleListVHostsCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        _ = command; // No additional parameters needed for listing virtual hosts

        // Get list of virtual hosts
        const vhost_names = self.listVirtualHosts(self.allocator) catch |err| {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error listing virtual hosts: {}\n", .{err});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        };
        defer {
            for (vhost_names) |name| {
                self.allocator.free(name);
            }
            self.allocator.free(vhost_names);
        }

        // Format and send response
        const header_msg = try std.fmt.allocPrint(self.allocator, "Virtual hosts ({}):\n", .{vhost_names.len});
        defer self.allocator.free(header_msg);
        try connection.stream.writeAll(header_msg);

        for (vhost_names) |vhost_name| {
            const vhost = self.getVirtualHost(vhost_name).?;
            const vhost_info = try std.fmt.allocPrint(self.allocator, "  {s} (queues: {}, exchanges: {}, active: {})\n", .{
                vhost_name,
                vhost.getQueueCount(),
                vhost.getExchangeCount(),
                vhost.active,
            });
            defer self.allocator.free(vhost_info);
            try connection.stream.writeAll(vhost_info);
        }
    }

    fn handleVHostCreateCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        // Parse command to extract virtual host name
        var parts = std.mem.splitSequence(u8, command, " ");
        _ = parts.next(); // "vhost"
        _ = parts.next(); // "create"

        const vhost_name = parts.next() orelse {
            const error_msg = "Error: Virtual host name is required. Usage: vhost create <name>\n";
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Create the virtual host
        self.createVirtualHost(vhost_name) catch |err| {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error creating virtual host '{s}': {}\n", .{ vhost_name, err });
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Send success response
        const success_msg = try std.fmt.allocPrint(self.allocator,
            \\Virtual host created successfully: {s}
            \\  Active: true
            \\  Queues: 0
            \\  Exchanges: {} (default exchanges)
            \\
        , .{
            vhost_name,
            self.getVirtualHost(vhost_name).?.getExchangeCount(),
        });
        defer self.allocator.free(success_msg);
        try connection.stream.writeAll(success_msg);
    }

    fn handleVHostDeleteCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        // Parse command to extract virtual host name
        var parts = std.mem.splitSequence(u8, command, " ");
        _ = parts.next(); // "vhost"
        _ = parts.next(); // "delete"

        const vhost_name = parts.next() orelse {
            const error_msg = "Error: Virtual host name is required. Usage: vhost delete <name>\n";
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Check if virtual host exists
        const vhost = self.getVirtualHost(vhost_name);
        if (vhost == null) {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Virtual host '{s}' not found\n", .{vhost_name});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        }

        // Delete the virtual host
        self.deleteVirtualHost(vhost_name) catch |err| {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error deleting virtual host '{s}': {}\n", .{ vhost_name, err });
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Send success response
        const success_msg = try std.fmt.allocPrint(self.allocator,
            \\Virtual host deleted successfully: {s}
            \\
        , .{vhost_name});
        defer self.allocator.free(success_msg);
        try connection.stream.writeAll(success_msg);
    }

    fn handleVHostInfoCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        // Parse command to extract virtual host name
        var parts = std.mem.splitSequence(u8, command, " ");
        _ = parts.next(); // "vhost"
        _ = parts.next(); // "info"

        const vhost_name = parts.next() orelse {
            const error_msg = "Error: Virtual host name is required. Usage: vhost info <name>\n";
            try connection.stream.writeAll(error_msg);
            return;
        };

        // Get the virtual host
        const vhost = self.getVirtualHost(vhost_name);
        if (vhost == null) {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error: Virtual host '{s}' not found\n", .{vhost_name});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        }

        // Get virtual host statistics
        var stats = vhost.?.getStats(self.allocator) catch |err| {
            const error_msg = try std.fmt.allocPrint(self.allocator, "Error getting virtual host stats: {}\n", .{err});
            defer self.allocator.free(error_msg);
            try connection.stream.writeAll(error_msg);
            return;
        };
        defer stats.object.deinit();

        // Format and send detailed virtual host information
        const vhost_info = try std.fmt.allocPrint(self.allocator,
            \\Virtual Host Information: {s}
            \\  Active: {}
            \\  Queues: {}
            \\  Exchanges: {}
            \\  Total Messages: {}
            \\  Total Consumers: {}
            \\
        , .{
            vhost_name,
            vhost.?.active,
            vhost.?.getQueueCount(),
            vhost.?.getExchangeCount(),
            stats.object.get("total_messages").?.integer,
            stats.object.get("total_consumers").?.integer,
        });
        defer self.allocator.free(vhost_info);
        try connection.stream.writeAll(vhost_info);
    }

    fn handleMetricsCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        _ = command; // No additional parameters needed for metrics

        if (self.metrics_registry) |*registry| {
            const metrics_json = registry.getAllMetrics(self.allocator) catch |err| {
                const error_msg = try std.fmt.allocPrint(self.allocator, "Error getting metrics: {}\n", .{err});
                defer self.allocator.free(error_msg);
                try connection.stream.writeAll(error_msg);
                return;
            };
            defer {
                var mutable_map = metrics_json.object;
                mutable_map.deinit();
            }

            const json_string = std.json.stringifyAlloc(self.allocator, metrics_json, .{ .whitespace = .indent_2 }) catch |err| {
                const error_msg = try std.fmt.allocPrint(self.allocator, "Error formatting metrics: {}\n", .{err});
                defer self.allocator.free(error_msg);
                try connection.stream.writeAll(error_msg);
                return;
            };
            defer self.allocator.free(json_string);

            const metrics_header = try std.fmt.allocPrint(self.allocator,
                \\Server Metrics:
                \\  Metrics HTTP Server: http://{s}:{}
                \\  Prometheus Endpoint: http://{s}:{}/metrics
                \\  JSON Endpoint: http://{s}:{}/metrics/json
                \\  Health Check: http://{s}:{}/health
                \\
                \\Current Metrics:
                \\{s}
                \\
            , .{
                self.config.metrics.host, self.config.metrics.port,
                self.config.metrics.host, self.config.metrics.port,
                self.config.metrics.host, self.config.metrics.port,
                self.config.metrics.host, self.config.metrics.port,
                json_string,
            });
            defer self.allocator.free(metrics_header);
            try connection.stream.writeAll(metrics_header);
        } else {
            const no_metrics_msg = "Metrics collection is disabled. Enable metrics in configuration to view metrics.\n";
            try connection.stream.writeAll(no_metrics_msg);
        }
    }

    fn handleConnectionWithRecovery(self: *Server, client_socket: std.net.Server.Connection) void {
        self.handleConnection(client_socket) catch {
            const error_info = ErrorHelpers.connectionError(.connection_forced, "Failed to initialize connection", 0);
            const action = self.error_handler.handleError(error_info);

            switch (action) {
                .shutdown_server => {
                    std.log.err("Shutting down server due to critical connection handling failure", .{});
                    self.requestShutdown();
                },
                else => {
                    // Close the socket and continue
                    client_socket.stream.close();
                },
            }
        };
    }

    fn handleConnection(self: *Server, client_socket: std.net.Server.Connection) !void {
        const connection = self.allocator.create(NetworkConnection) catch |err| {
            const error_info = ErrorHelpers.fatalError(.resource_error, "Failed to allocate memory for connection");
            const action = self.error_handler.handleError(error_info);

            if (action == .shutdown_server) {
                self.requestShutdown();
                return error.OutOfMemory;
            }
            return err;
        };

        connection.* = NetworkConnection.init(self.allocator, self.next_connection_id, client_socket.stream) catch |err| {
            self.allocator.destroy(connection);

            const error_info = ErrorHelpers.connectionError(.connection_forced, "Failed to initialize connection", self.next_connection_id);
            _ = self.error_handler.handleError(error_info);

            return err;
        };

        const connection_id = connection.id;
        self.next_connection_id += 1;

        self.connections.append(connection) catch |err| {
            connection.deinit();
            self.allocator.destroy(connection);

            const error_info = ErrorHelpers.fatalError(.resource_error, "Failed to track connection");
            const action = self.error_handler.handleError(error_info);

            if (action == .shutdown_server) {
                self.requestShutdown();
            }
            return err;
        };

        std.log.info("New connection established: {}", .{connection_id});

        // Use protocol handler for connection management with error recovery
        self.protocol_handler.handleConnection(connection) catch |err| {
            const error_info = ErrorHelpers.connectionError(.connection_forced, "Protocol handling failed", connection_id);
            const action = self.error_handler.handleError(error_info);

            switch (action) {
                .shutdown_server => self.requestShutdown(),
                else => {}, // Connection will be cleaned up by removeConnection
            }

            self.removeConnection(connection);
            return err;
        };
    }

    fn removeConnection(self: *Server, connection: *NetworkConnection) void {
        for (self.connections.items, 0..) |conn, i| {
            if (conn.id == connection.id) {
                _ = self.connections.swapRemove(i);
                conn.deinit();
                self.allocator.destroy(conn);
                break;
            }
        }
        std.log.info("Connection {} removed", .{connection.id});
    }

    pub fn createVirtualHost(self: *Server, name: []const u8) !void {
        const owned_name = try self.allocator.dupe(u8, name);
        const vhost = try self.allocator.create(VirtualHost);
        vhost.* = try VirtualHost.init(self.allocator, owned_name);

        // Set server reference for cluster operations
        vhost.setServerReference(self);

        try self.vhosts.put(owned_name, vhost);
        std.log.info("Virtual host created: {s}", .{name});
    }

    pub fn getVirtualHost(self: *Server, name: []const u8) ?*VirtualHost {
        return self.vhosts.get(name);
    }

    pub fn deleteVirtualHost(self: *Server, name: []const u8) !void {
        // Don't allow deleting the default virtual host
        if (std.mem.eql(u8, name, "/")) {
            return error.CannotDeleteDefaultVHost;
        }

        if (self.vhosts.fetchRemove(name)) |entry| {
            entry.value.deinit();
            self.allocator.destroy(entry.value);
            self.allocator.free(entry.key);
            std.log.info("Virtual host deleted: {s}", .{name});
        } else {
            return error.VirtualHostNotFound;
        }
    }

    pub fn getConnectionCount(self: *const Server) u32 {
        return @intCast(self.connections.items.len);
    }

    pub fn getVirtualHostCount(self: *const Server) u32 {
        return @intCast(self.vhosts.count());
    }

    pub fn listVirtualHosts(self: *Server, allocator: std.mem.Allocator) ![][]const u8 {
        var vhost_list = std.ArrayList([]const u8).init(allocator);
        defer vhost_list.deinit();

        var iterator = self.vhosts.iterator();
        while (iterator.next()) |entry| {
            try vhost_list.append(try allocator.dupe(u8, entry.key_ptr.*));
        }

        return vhost_list.toOwnedSlice();
    }

    /// Request graceful shutdown
    pub fn requestShutdown(self: *Server) void {
        self.shutdown_requested.store(true, .monotonic);
        std.log.info("Graceful shutdown requested", .{});
    }

    /// Check if shutdown has been requested
    pub fn isShutdownRequested(self: *const Server) bool {
        return self.shutdown_requested.load(.monotonic);
    }

    // Metrics and statistics
    pub fn getStats(self: *const Server, allocator: std.mem.Allocator) !std.json.Value {
        var stats = std.json.ObjectMap.init(allocator);

        try stats.put("connections", std.json.Value{ .integer = @intCast(self.connections.items.len) });
        try stats.put("virtual_hosts", std.json.Value{ .integer = @intCast(self.vhosts.count()) });
        try stats.put("running", std.json.Value{ .bool = self.running });
        try stats.put("shutdown_requested", std.json.Value{ .bool = self.shutdown_requested.load(.monotonic) });

        // Include error statistics
        const error_stats = try self.error_handler.getErrorStats(allocator);
        try stats.put("errors", error_stats);

        return std.json.Value{ .object = stats };
    }

    fn recoverPersistentState(self: *Server) !void {
        const persistence_dir = "./yak_persistence";

        // Ensure persistence directory exists
        std.fs.cwd().makeDir(persistence_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        // Recover virtual hosts and their durable resources
        var vhost_iterator = self.vhosts.iterator();
        while (vhost_iterator.next()) |entry| {
            try self.recoverVirtualHost(entry.value_ptr.*, persistence_dir);
        }

        std.log.info("Persistent state recovery completed", .{});
    }

    fn recoverVirtualHost(self: *Server, vhost: *VirtualHost, persistence_dir: []const u8) !void {
        const vhost_dir = try std.fmt.allocPrint(self.allocator, "{s}/vhost_{s}", .{ persistence_dir, vhost.name });
        defer self.allocator.free(vhost_dir);

        // Try to open vhost directory
        var dir = std.fs.cwd().openDir(vhost_dir, .{}) catch |err| switch (err) {
            error.FileNotFound => {
                return;
            },
            else => return err,
        };
        defer dir.close();

        // Recover durable queues and their messages
        var queue_iterator = vhost.queues.iterator();
        while (queue_iterator.next()) |entry| {
            const queue = entry.value_ptr.*;
            if (queue.durable) {
                try self.recoverQueue(queue, dir);
            }
        }
    }

    fn recoverQueue(self: *Server, queue: *Queue, vhost_dir: std.fs.Dir) !void {
        const queue_file = try std.fmt.allocPrint(self.allocator, "queue_{s}.dat", .{queue.name});
        defer self.allocator.free(queue_file);

        const file = vhost_dir.openFile(queue_file, .{}) catch |err| switch (err) {
            error.FileNotFound => {
                return;
            },
            else => return err,
        };
        defer file.close();

        const file_size = try file.getEndPos();
        if (file_size == 0) return;

        const file_data = try self.allocator.alloc(u8, file_size);
        defer self.allocator.free(file_data);

        _ = try file.readAll(file_data);

        // Parse message count from file header
        if (file_data.len < 4) return;
        const message_count = std.mem.readInt(u32, file_data[0..4], .little);

        var offset: usize = 4;
        var recovered_count: u32 = 0;

        for (0..message_count) |_| {
            if (offset >= file_data.len) break;

            // Read message size
            if (offset + 4 > file_data.len) break;
            const message_size = std.mem.readInt(u32, file_data[offset .. offset + 4][0..4], .little);
            offset += 4;

            if (offset + message_size > file_data.len) break;

            // Deserialize message
            const message_data = file_data[offset .. offset + message_size];
            var message = Message.decodeFromStorage(message_data, self.allocator) catch |err| {
                std.log.warn("Failed to decode persistent message: {}", .{err});
                offset += message_size;
                continue;
            };

            // Add message to queue
            queue.publish(message) catch {
                const error_info = ErrorHelpers.recoverableError(.resource_error, "Failed to restore persistent message to queue");
                _ = self.error_handler.handleError(error_info);
                message.deinit();
            };

            recovered_count += 1;
            offset += message_size;
        }

        std.log.info("Recovered {} persistent messages for queue {s}", .{ recovered_count, queue.name });
    }

    pub fn persistMessage(self: *Server, vhost_name: []const u8, queue_name: []const u8, message: *const Message) !void {
        if (!message.persistent) return;

        self.persistence_mutex.lock();
        defer self.persistence_mutex.unlock();

        const persistence_dir = "./yak_persistence";
        const vhost_dir_path = try std.fmt.allocPrint(self.allocator, "{s}/vhost_{s}", .{ persistence_dir, vhost_name });
        defer self.allocator.free(vhost_dir_path);

        // Ensure vhost directory exists
        std.fs.cwd().makeDir(vhost_dir_path) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const queue_file_path = try std.fmt.allocPrint(self.allocator, "{s}/queue_{s}.dat", .{ vhost_dir_path, queue_name });
        defer self.allocator.free(queue_file_path);

        // Serialize and append message to queue file
        const encoded_message = try message.encodeForStorage(self.allocator);
        defer self.allocator.free(encoded_message);

        const file = try std.fs.cwd().createFile(queue_file_path, .{ .truncate = false });
        defer file.close();

        // Check if file is new (needs header)
        const file_size = try file.getEndPos();
        if (file_size == 0) {
            // Write initial message count (0, will be updated later)
            var buffer: [4]u8 = undefined;
            std.mem.writeInt(u32, &buffer, 0, .little);
            try file.writeAll(&buffer);
        }

        // Seek to end and append message
        try file.seekTo(file_size);
        var buffer: [4]u8 = undefined;
        std.mem.writeInt(u32, &buffer, @intCast(encoded_message.len), .little);
        try file.writeAll(&buffer);
        try file.writeAll(encoded_message);

        // Update message count in header
        try file.seekTo(0);
        const current_count = if (file_size == 0) 1 else blk: {
            var read_buffer: [4]u8 = undefined;
            _ = try file.readAll(&read_buffer);
            const count = std.mem.readInt(u32, &read_buffer, .little);
            break :blk count + 1;
        };
        try file.seekTo(0);
        std.mem.writeInt(u32, &buffer, current_count, .little);
        try file.writeAll(&buffer);
    }

    pub fn persistQueueState(self: *Server, vhost_name: []const u8, queue: *const Queue) !void {
        if (!queue.durable) return;

        self.persistence_mutex.lock();
        defer self.persistence_mutex.unlock();

        const persistence_dir = "./yak_persistence";
        const vhost_dir_path = try std.fmt.allocPrint(self.allocator, "{s}/vhost_{s}", .{ persistence_dir, vhost_name });
        defer self.allocator.free(vhost_dir_path);

        // Ensure vhost directory exists
        std.fs.cwd().makeDir(vhost_dir_path) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const queue_file_path = try std.fmt.allocPrint(self.allocator, "{s}/queue_{s}.dat", .{ vhost_dir_path, queue.name });
        defer self.allocator.free(queue_file_path);

        // Rewrite entire queue file with current persistent messages
        const file = try std.fs.cwd().createFile(queue_file_path, .{ .truncate = true });
        defer file.close();

        var persistent_count: u32 = 0;
        for (queue.messages.items) |message| {
            if (message.persistent) persistent_count += 1;
        }

        // Write message count header
        var buffer: [4]u8 = undefined;
        std.mem.writeInt(u32, &buffer, persistent_count, .little);
        try file.writeAll(&buffer);

        // Write persistent messages
        for (queue.messages.items) |message| {
            if (message.persistent) {
                const encoded = try message.encodeForStorage(self.allocator);
                defer self.allocator.free(encoded);

                std.mem.writeInt(u32, &buffer, @intCast(encoded.len), .little);
                try file.writeAll(&buffer);
                try file.writeAll(encoded);
            }
        }
    }

    /// Periodic maintenance for error handler
    pub fn performMaintenance(self: *Server) void {
        // Clear old errors (older than 1 hour)
        self.error_handler.clearOldErrors(3600);
    }

    /// Initialize metrics collection and HTTP server
    fn initializeSsl(self: *Server) !void {
        // Initialize SSL context
        self.ssl_context = try SslContext.init(self.config.ssl, self.allocator);

        // Create SSL listener
        const ssl_address = try std.net.Address.parseIp(self.config.tcp.host, self.config.ssl.port);
        self.ssl_listener = try self.ssl_context.?.listen(ssl_address);

        // Start SSL server thread
        self.ssl_thread = try std.Thread.spawn(.{}, sslServerThread, .{self});

        std.log.info("SSL/TLS server initialized - listening on {s}:{d}", .{ self.config.tcp.host, self.config.ssl.port });
    }

    fn sslServerThread(self: *Server) void {
        std.log.info("SSL server thread started", .{});

        while (self.running and !self.shutdown_requested.load(.monotonic)) {
            if (self.ssl_listener) |*ssl_listener| {
                const ssl_connection = ssl_listener.acceptTimeout(100) catch |err| switch (err) {
                    error.WouldBlock => continue,
                    else => {
                        std.log.warn("Failed to accept SSL connection: {}", .{err});
                        continue;
                    },
                };

                if (ssl_connection) |ssl_conn| {
                    // Handle SSL connection
                    self.handleSslConnection(ssl_conn);
                }
            }
        }

        std.log.info("SSL server thread stopped", .{});
    }

    fn handleSslConnection(self: *Server, mut_ssl_conn: @import("../network/ssl.zig").SslConnection) void {
        var ssl_conn = mut_ssl_conn;
        defer ssl_conn.deinit();

        // Perform SSL handshake
        ssl_conn.handshake(self.allocator) catch |err| {
            std.log.warn("SSL handshake failed: {}", .{err});
            return;
        };

        // Create network connection wrapper
        const connection_id = self.next_connection_id;
        self.next_connection_id += 1;

        var network_connection = NetworkConnection.init(self.allocator, connection_id, ssl_conn.stream) catch |err| {
            std.log.warn("Failed to create network connection: {}", .{err});
            return;
        };
        defer network_connection.deinit();

        // Add to connections list
        self.connections.append(&network_connection) catch |err| {
            std.log.warn("Failed to add SSL connection to list: {}", .{err});
            return;
        };

        std.log.info("SSL connection established: {} from {any}", .{ connection_id, ssl_conn.address });

        // Handle connection with protocol handler
        self.protocol_handler.handleConnection(&network_connection) catch |err| {
            std.log.warn("SSL connection protocol handling failed: {}", .{err});
            return;
        };

        // Remove from connections list
        for (self.connections.items, 0..) |conn, i| {
            if (conn.id == connection_id) {
                _ = self.connections.orderedRemove(i);
                break;
            }
        }

        std.log.info("SSL connection closed: {}", .{connection_id});
    }

    fn initializeMetrics(self: *Server) !void {
        // Initialize metrics registry
        self.metrics_registry = MetricsRegistry.init(self.allocator);

        // Initialize HTTP server
        self.metrics_http_server = try MetricsHttpServer.init(self.allocator, &self.metrics_registry.?, self.config.metrics.host, self.config.metrics.port);

        // Initialize metrics collector
        self.metrics_collector = BrokerMetricsCollector.init(&self.metrics_registry.?, self, self.config.metrics.collection_interval_ms);

        // Start metrics collection
        try self.metrics_collector.?.start();

        // Start HTTP server
        try self.metrics_http_server.?.start();

        std.log.info("Metrics system initialized - HTTP server on {s}:{d}", .{ self.config.metrics.host, self.config.metrics.port });
    }

    /// Initialize cluster components
    fn initializeCluster(self: *Server) !void {
        // Initialize cluster
        self.cluster = try Cluster.init(self.allocator, self.config.cluster);

        std.log.info("Cluster system initialized - node {} on {s}:{d}", .{ self.config.cluster.node_id, self.config.cluster.bind_address, self.config.cluster.bind_port });
    }

    fn handleClusterStatusCommand(self: *Server, connection: std.net.Server.Connection, command: []const u8) !void {
        _ = command; // No additional parameters needed for cluster status

        if (self.cluster) |*cluster| {
            const cluster_status = cluster.getClusterStatus();

            const status_msg = try std.fmt.allocPrint(self.allocator,
                \\Cluster Status:
                \\  Enabled: {}
                \\  Node Count: {}
                \\  Leader Node: {}
                \\  Is Leader: {}
                \\  Active Connections: {}
                \\  Local Node ID: {}
                \\  Bind Address: {s}:{}
                \\  Replication Factor: {}
                \\  Auto Failover: {}
                \\
            , .{
                cluster_status.enabled,
                cluster_status.node_count,
                cluster_status.leader_node orelse 0,
                cluster_status.is_leader,
                cluster_status.active_connections,
                self.config.cluster.node_id,
                self.config.cluster.bind_address,
                self.config.cluster.bind_port,
                self.config.cluster.replication_factor,
                self.config.cluster.enable_auto_failover,
            });
            defer self.allocator.free(status_msg);
            try connection.stream.writeAll(status_msg);
        } else {
            const no_cluster_msg = "Cluster support is disabled. Enable clustering in configuration to view cluster status.\n";
            try connection.stream.writeAll(no_cluster_msg);
        }
    }

    /// Get metrics registry for external access
    pub fn getMetricsRegistry(self: *Server) ?*MetricsRegistry {
        return if (self.metrics_registry) |*registry| registry else null;
    }

    /// Record connection opened metric
    pub fn recordConnectionOpened(self: *Server) void {
        if (self.metrics_registry) |*registry| {
            registry.incrementCounter("amqp_connections_opened_total", 1) catch {};
        }
    }

    /// Record connection closed metric
    pub fn recordConnectionClosed(self: *Server) void {
        if (self.metrics_registry) |*registry| {
            registry.incrementCounter("amqp_connections_closed_total", 1) catch {};
        }
    }

    /// Record message published metric
    pub fn recordMessagePublished(self: *Server, bytes: usize) void {
        if (self.metrics_registry) |*registry| {
            registry.incrementCounter("amqp_messages_published_total", 1) catch {};
            registry.incrementCounter("amqp_bytes_received_total", bytes) catch {};
        }
    }

    /// Record message delivered metric
    pub fn recordMessageDelivered(self: *Server, bytes: usize) void {
        if (self.metrics_registry) |*registry| {
            registry.incrementCounter("amqp_messages_delivered_total", 1) catch {};
            registry.incrementCounter("amqp_bytes_sent_total", bytes) catch {};
        }
    }

    /// Record message acknowledged metric
    pub fn recordMessageAcknowledged(self: *Server) void {
        if (self.metrics_registry) |*registry| {
            registry.incrementCounter("amqp_messages_acknowledged_total", 1) catch {};
        }
    }

    /// Record message rejected metric
    pub fn recordMessageRejected(self: *Server) void {
        if (self.metrics_registry) |*registry| {
            registry.incrementCounter("amqp_messages_rejected_total", 1) catch {};
        }
    }

    /// Record processing duration metric
    pub fn recordProcessingDuration(self: *Server, metric_name: []const u8, duration_ns: u64) void {
        if (self.metrics_registry) |*registry| {
            const duration_seconds = @as(f64, @floatFromInt(duration_ns)) / 1_000_000_000.0;
            registry.observeHistogram(metric_name, duration_seconds) catch {};
        }
    }

    /// Record error metric
    pub fn recordError(self: *Server, error_type: []const u8) void {
        if (self.metrics_registry) |*registry| {
            registry.incrementCounter("amqp_errors_total", 1) catch {};

            if (std.mem.eql(u8, error_type, "protocol")) {
                registry.incrementCounter("amqp_protocol_errors_total", 1) catch {};
            } else if (std.mem.eql(u8, error_type, "connection")) {
                registry.incrementCounter("amqp_connection_errors_total", 1) catch {};
            }
        }
    }
};

test "server creation and basic operations" {
    const allocator = std.testing.allocator;

    var config = try Config.default(allocator);
    defer config.deinit(allocator);

    var server = try Server.init(allocator, config);
    defer server.deinit();

    try std.testing.expectEqual(@as(u32, 1), server.getVirtualHostCount());
    try std.testing.expectEqual(@as(u32, 0), server.getConnectionCount());

    try server.createVirtualHost("test");
    try std.testing.expectEqual(@as(u32, 2), server.getVirtualHostCount());

    const vhost = server.getVirtualHost("/");
    try std.testing.expect(vhost != null);
}

test "server queue info command handling" {
    const allocator = std.testing.allocator;

    var config = try Config.default(allocator);
    defer config.deinit(allocator);

    var server = try Server.init(allocator, config);
    defer server.deinit();

    // Create a test queue in the default virtual host
    const vhost = server.getVirtualHost("/").?;
    const queue_name = try vhost.declareQueue("test-info-queue", true, false, false, null);
    try std.testing.expectEqualStrings("test-info-queue", queue_name);

    // Verify queue was created
    const queue = vhost.getQueue("test-info-queue");
    try std.testing.expect(queue != null);
    try std.testing.expectEqual(true, queue.?.durable);
    try std.testing.expectEqual(false, queue.?.exclusive);
    try std.testing.expectEqual(false, queue.?.auto_delete);
}

test "server queue declare command handling" {
    const allocator = std.testing.allocator;

    var config = try Config.default(allocator);
    defer config.deinit(allocator);

    var server = try Server.init(allocator, config);
    defer server.deinit();

    const vhost = server.getVirtualHost("/").?;

    // Test declaring a basic queue
    const queue_name1 = try vhost.declareQueue("test-declare-basic", false, false, false, null);
    try std.testing.expectEqualStrings("test-declare-basic", queue_name1);

    const queue1 = vhost.getQueue("test-declare-basic");
    try std.testing.expect(queue1 != null);
    try std.testing.expectEqual(false, queue1.?.durable);
    try std.testing.expectEqual(false, queue1.?.exclusive);
    try std.testing.expectEqual(false, queue1.?.auto_delete);

    // Test declaring a durable queue
    const queue_name2 = try vhost.declareQueue("test-declare-durable", true, false, false, null);
    try std.testing.expectEqualStrings("test-declare-durable", queue_name2);

    const queue2 = vhost.getQueue("test-declare-durable");
    try std.testing.expect(queue2 != null);
    try std.testing.expectEqual(true, queue2.?.durable);
    try std.testing.expectEqual(false, queue2.?.exclusive);
    try std.testing.expectEqual(false, queue2.?.auto_delete);

    // Test declaring an exclusive auto-delete queue
    const queue_name3 = try vhost.declareQueue("test-declare-exclusive", false, true, true, null);
    try std.testing.expectEqualStrings("test-declare-exclusive", queue_name3);

    const queue3 = vhost.getQueue("test-declare-exclusive");
    try std.testing.expect(queue3 != null);
    try std.testing.expectEqual(false, queue3.?.durable);
    try std.testing.expectEqual(true, queue3.?.exclusive);
    try std.testing.expectEqual(true, queue3.?.auto_delete);
}

test "server queue delete command handling" {
    const allocator = std.testing.allocator;

    var config = try Config.default(allocator);
    defer config.deinit(allocator);

    var server = try Server.init(allocator, config);
    defer server.deinit();

    const vhost = server.getVirtualHost("/").?;

    // Create test queues
    _ = try vhost.declareQueue("test-delete-basic", false, false, false, null);
    _ = try vhost.declareQueue("test-delete-durable", true, false, false, null);
    _ = try vhost.declareQueue("test-delete-exclusive", false, true, false, null);

    // Verify queues exist
    try std.testing.expect(vhost.getQueue("test-delete-basic") != null);
    try std.testing.expect(vhost.getQueue("test-delete-durable") != null);
    try std.testing.expect(vhost.getQueue("test-delete-exclusive") != null);
    try std.testing.expectEqual(@as(u32, 3), vhost.getQueueCount());

    // Test deleting basic queue
    const message_count1 = try vhost.deleteQueue("test-delete-basic", false, false);
    try std.testing.expectEqual(@as(u32, 0), message_count1);
    try std.testing.expect(vhost.getQueue("test-delete-basic") == null);
    try std.testing.expectEqual(@as(u32, 2), vhost.getQueueCount());

    // Test deleting durable queue
    const message_count2 = try vhost.deleteQueue("test-delete-durable", false, false);
    try std.testing.expectEqual(@as(u32, 0), message_count2);
    try std.testing.expect(vhost.getQueue("test-delete-durable") == null);
    try std.testing.expectEqual(@as(u32, 1), vhost.getQueueCount());

    // Test deleting exclusive queue
    const message_count3 = try vhost.deleteQueue("test-delete-exclusive", false, false);
    try std.testing.expectEqual(@as(u32, 0), message_count3);
    try std.testing.expect(vhost.getQueue("test-delete-exclusive") == null);
    try std.testing.expectEqual(@as(u32, 0), vhost.getQueueCount());
}

test "server queue purge command handling" {
    const allocator = std.testing.allocator;

    var config = try Config.default(allocator);
    defer config.deinit(allocator);

    var server = try Server.init(allocator, config);
    defer server.deinit();

    const vhost = server.getVirtualHost("/").?;

    // Create test queue
    _ = try vhost.declareQueue("test-purge-queue", false, false, false, null);

    // Verify queue exists
    const queue = vhost.getQueue("test-purge-queue");
    try std.testing.expect(queue != null);

    // Test purging empty queue
    const message_count1 = try vhost.purgeQueue("test-purge-queue");
    try std.testing.expectEqual(@as(u32, 0), message_count1);

    // Verify queue still exists after purge
    try std.testing.expect(vhost.getQueue("test-purge-queue") != null);
    try std.testing.expectEqual(@as(u32, 1), vhost.getQueueCount());
}

test "server exchange commands handling" {
    const allocator = std.testing.allocator;

    var config = try Config.default(allocator);
    defer config.deinit(allocator);

    var server = try Server.init(allocator, config);
    defer server.deinit();

    const vhost = server.getVirtualHost("/").?;

    // Check that default exchanges exist (created during vhost init)
    const initial_count = vhost.getExchangeCount();
    try std.testing.expect(initial_count > 0);

    // Test declaring a custom direct exchange
    try vhost.declareExchange("test-direct", @import("../routing/exchange.zig").ExchangeType.direct, true, false, false, null);
    try std.testing.expectEqual(initial_count + 1, vhost.getExchangeCount());

    const exchange1 = vhost.getExchange("test-direct");
    try std.testing.expect(exchange1 != null);
    try std.testing.expectEqual(@import("../routing/exchange.zig").ExchangeType.direct, exchange1.?.exchange_type);
    try std.testing.expectEqual(true, exchange1.?.durable);
    try std.testing.expectEqual(false, exchange1.?.auto_delete);

    // Test declaring a fanout exchange
    try vhost.declareExchange("test-fanout", @import("../routing/exchange.zig").ExchangeType.fanout, false, true, false, null);
    try std.testing.expectEqual(initial_count + 2, vhost.getExchangeCount());

    const exchange2 = vhost.getExchange("test-fanout");
    try std.testing.expect(exchange2 != null);
    try std.testing.expectEqual(@import("../routing/exchange.zig").ExchangeType.fanout, exchange2.?.exchange_type);
    try std.testing.expectEqual(false, exchange2.?.durable);
    try std.testing.expectEqual(true, exchange2.?.auto_delete);

    // Test deleting exchanges
    try vhost.deleteExchange("test-direct", false);
    try std.testing.expectEqual(initial_count + 1, vhost.getExchangeCount());
    try std.testing.expect(vhost.getExchange("test-direct") == null);

    try vhost.deleteExchange("test-fanout", false);
    try std.testing.expectEqual(initial_count, vhost.getExchangeCount());
    try std.testing.expect(vhost.getExchange("test-fanout") == null);
}

test "server connection listing functionality" {
    const allocator = std.testing.allocator;

    var config = try Config.default(allocator);
    defer config.deinit(allocator);

    var server = try Server.init(allocator, config);
    defer server.deinit();

    // Initially no connections
    try std.testing.expectEqual(@as(u32, 0), server.getConnectionCount());

    // The connections list should be empty
    try std.testing.expectEqual(@as(usize, 0), server.connections.items.len);
}

test "server virtual host management" {
    const allocator = std.testing.allocator;

    var config = try Config.default(allocator);
    defer config.deinit(allocator);

    var server = try Server.init(allocator, config);
    defer server.deinit();

    // Should start with default virtual host
    try std.testing.expectEqual(@as(u32, 1), server.getVirtualHostCount());
    try std.testing.expect(server.getVirtualHost("/") != null);

    // Test creating virtual hosts
    try server.createVirtualHost("test1");
    try server.createVirtualHost("test2");
    try std.testing.expectEqual(@as(u32, 3), server.getVirtualHostCount());

    // Test getting virtual hosts
    try std.testing.expect(server.getVirtualHost("test1") != null);
    try std.testing.expect(server.getVirtualHost("test2") != null);
    try std.testing.expect(server.getVirtualHost("nonexistent") == null);

    // Test listing virtual hosts
    const vhost_names = try server.listVirtualHosts(allocator);
    defer {
        for (vhost_names) |name| {
            allocator.free(name);
        }
        allocator.free(vhost_names);
    }
    try std.testing.expectEqual(@as(usize, 3), vhost_names.len);

    // Test deleting virtual hosts
    try server.deleteVirtualHost("test1");
    try std.testing.expectEqual(@as(u32, 2), server.getVirtualHostCount());
    try std.testing.expect(server.getVirtualHost("test1") == null);

    // Test that we can't delete the default virtual host
    try std.testing.expectError(error.CannotDeleteDefaultVHost, server.deleteVirtualHost("/"));
    try std.testing.expectEqual(@as(u32, 2), server.getVirtualHostCount());

    // Test deleting non-existent virtual host
    try std.testing.expectError(error.VirtualHostNotFound, server.deleteVirtualHost("nonexistent"));
}
