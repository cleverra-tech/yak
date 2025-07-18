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
    cli_server: ?std.net.Server,
    cli_thread: ?std.Thread,
    protocol_handler: ProtocolHandler,
    persistence_mutex: std.Thread.Mutex,
    error_handler: ErrorHandler,
    shutdown_requested: std.atomic.Value(bool),

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
            .cli_server = null,
            .cli_thread = null,
            .protocol_handler = protocol_handler,
            .persistence_mutex = std.Thread.Mutex{},
            .error_handler = ErrorHandler.init(allocator),
            .shutdown_requested = std.atomic.Value(bool).init(false),
        };

        // Set up global server reference for persistence callback
        global_server = &server;

        // Set up persistence callback in protocol handler
        server.protocol_handler.persist_message_fn = serverPersistMessage;

        // Set up error handler callback in protocol handler
        server.protocol_handler.error_handler_fn = serverHandleError;

        // Create default virtual host
        try server.createVirtualHost("/");

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
        std.log.info("Starting Yak AMQP server on {s}:{}", .{ self.config.tcp.host, self.config.tcp.port });

        const address = try std.net.Address.parseIp(self.config.tcp.host, self.config.tcp.port);
        self.tcp_server = try address.listen(.{
            .reuse_address = true,
        });

        self.running = true;

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
        std.log.info("Starting Yak AMQP server on {s}:{}", .{ self.config.tcp.host, self.config.tcp.port });

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
        std.log.debug("CLI server thread started", .{});

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

                std.log.debug("Spawned CLI client thread for client {}", .{client_id});
            }
        }

        std.log.debug("CLI server thread shutting down", .{});
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
        std.log.debug("Processing CLI client connection", .{});

        // For now, send a simple welcome message and close
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
                const help_msg = "Available commands:\n  help - Show this help\n  status - Show server status\n  list queues [vhost] - List all queues in virtual host (default: /)\n  exit - Close connection\n";
                try connection.stream.writeAll(help_msg);
            } else if (std.mem.startsWith(u8, command, "list queues")) {
                try self.handleListQueuesCommand(connection, command);
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

        try self.vhosts.put(owned_name, vhost);
        std.log.info("Virtual host created: {s}", .{name});
    }

    pub fn getVirtualHost(self: *Server, name: []const u8) ?*VirtualHost {
        return self.vhosts.get(name);
    }

    pub fn getConnectionCount(self: *const Server) u32 {
        return @intCast(self.connections.items.len);
    }

    pub fn getVirtualHostCount(self: *const Server) u32 {
        return @intCast(self.vhosts.count());
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
                std.log.debug("No persistent data found for vhost {s}", .{vhost.name});
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
                std.log.debug("No persistent messages found for queue {s}", .{queue.name});
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

        std.log.debug("Persisted message {} to queue {s} in vhost {s}", .{ message.id, queue_name, vhost_name });
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

        std.log.debug("Persisted {} messages for durable queue {s} in vhost {s}", .{ persistent_count, queue.name, vhost_name });
    }

    /// Periodic maintenance for error handler
    pub fn performMaintenance(self: *Server) void {
        // Clear old errors (older than 1 hour)
        self.error_handler.clearOldErrors(3600);

        std.log.debug("Performed server maintenance: cleared old errors", .{});
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
