const std = @import("std");
const Config = @import("../config.zig").Config;
const VirtualHost = @import("vhost.zig").VirtualHost;
const NetworkConnection = @import("../network/connection.zig").Connection;
const ProtocolHandler = @import("../network/protocol.zig").ProtocolHandler;
const Message = @import("../message.zig").Message;
const Queue = @import("../routing/queue.zig").Queue;
const Exchange = @import("../routing/exchange.zig").Exchange;

// Global reference to server instance for persistence callback
var global_server: ?*Server = null;

fn serverPersistMessage(vhost_name: []const u8, queue_name: []const u8, message: *const Message) !void {
    if (global_server) |server| {
        try server.persistMessage(vhost_name, queue_name, message);
    }
}

pub const Server = struct {
    allocator: std.mem.Allocator,
    config: Config,
    vhosts: std.HashMap([]const u8, *VirtualHost, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    connections: std.ArrayList(*NetworkConnection),
    next_connection_id: u64,
    running: bool,
    tcp_server: ?std.net.Server,
    cli_server: ?std.net.Server,
    protocol_handler: ProtocolHandler,
    persistence_mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, config: Config) !Server {
        const protocol_handler = ProtocolHandler.init(allocator);

        var server = Server{
            .allocator = allocator,
            .config = config,
            .vhosts = std.HashMap([]const u8, *VirtualHost, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .connections = std.ArrayList(*NetworkConnection).init(allocator),
            .next_connection_id = 1,
            .running = false,
            .tcp_server = null,
            .cli_server = null,
            .protocol_handler = protocol_handler,
            .persistence_mutex = std.Thread.Mutex{},
        };

        // Set up global server reference for persistence callback
        global_server = &server;
        
        // Set up persistence callback in protocol handler
        server.protocol_handler.persist_message_fn = serverPersistMessage;

        // Create default virtual host
        try server.createVirtualHost("/");

        // Recover persistent state from storage
        server.recoverPersistentState() catch |err| {
            std.log.warn("Failed to recover persistent state: {}", .{err});
        };

        // Note: Virtual host integration will be handled within the protocol handler
        // using the connection's setVirtualHost method

        return server;
    }

    pub fn deinit(self: *Server) void {
        // Clear global reference
        global_server = null;
        
        self.stop();

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
        while (self.running) {
            const client_socket = self.tcp_server.?.accept() catch |err| {
                std.log.err("Failed to accept connection: {}", .{err});
                continue;
            };

            // Handle connection in a separate thread (simplified for now)
            self.handleConnection(client_socket) catch |err| {
                std.log.err("Failed to handle connection: {}", .{err});
                client_socket.stream.close();
            };
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
        while (self.running and !shutdown_flag.*) {
            // Use accept with timeout to periodically check shutdown flag
            const client_socket = self.acceptWithTimeout(100) catch |err| switch (err) {
                error.WouldBlock => continue, // Timeout, check shutdown flag
                else => {
                    std.log.err("Failed to accept connection: {}", .{err});
                    continue;
                },
            };

            if (client_socket) |socket| {
                // Handle connection in a separate thread (simplified for now)
                self.handleConnection(socket) catch |err| {
                    std.log.err("Failed to handle connection: {}", .{err});
                    socket.stream.close();
                };
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

        // TODO: Start CLI server in separate thread
    }

    fn handleConnection(self: *Server, client_socket: std.net.Server.Connection) !void {
        const connection = try self.allocator.create(NetworkConnection);
        connection.* = try NetworkConnection.init(self.allocator, self.next_connection_id, client_socket.stream);
        self.next_connection_id += 1;

        try self.connections.append(connection);

        std.log.info("New connection established: {}", .{connection.id});

        // Use protocol handler for connection management
        self.protocol_handler.handleConnection(connection) catch |err| {
            std.log.err("Protocol handling failed for connection {}: {}", .{ connection.id, err });
            self.removeConnection(connection);
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

    // Metrics and statistics
    pub fn getStats(self: *const Server, allocator: std.mem.Allocator) !std.json.Value {
        var stats = std.json.ObjectMap.init(allocator);

        try stats.put("connections", std.json.Value{ .integer = @intCast(self.connections.items.len) });
        try stats.put("virtual_hosts", std.json.Value{ .integer = @intCast(self.vhosts.count()) });
        try stats.put("running", std.json.Value{ .bool = self.running });

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
            const message_size = std.mem.readInt(u32, file_data[offset..offset + 4][0..4], .little);
            offset += 4;

            if (offset + message_size > file_data.len) break;
            
            // Deserialize message
            const message_data = file_data[offset..offset + message_size];
            var message = Message.decodeFromStorage(message_data, self.allocator) catch |err| {
                std.log.warn("Failed to decode persistent message: {}", .{err});
                offset += message_size;
                continue;
            };

            // Add message to queue
            queue.publish(message) catch |err| {
                std.log.warn("Failed to restore message to queue {s}: {}", .{ queue.name, err });
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
