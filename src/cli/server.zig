const std = @import("std");
const Config = @import("../config.zig").Config;

pub const CliServer = struct {
    socket_path: []const u8,
    server: ?std.net.Server,
    running: bool,
    allocator: std.mem.Allocator,
    clients: std.ArrayList(*CliClient),
    message_broker: ?*MessageBroker, // Reference to main broker

    const MessageBroker = @import("../core/server.zig").Server;

    const CliClient = struct {
        id: u64,
        stream: std.net.Stream,
        authenticated: bool,
        allocator: std.mem.Allocator,

        pub fn init(allocator: std.mem.Allocator, id: u64, stream: std.net.Stream) CliClient {
            return CliClient{
                .id = id,
                .stream = stream,
                .authenticated = false,
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *CliClient) void {
            self.stream.close();
        }

        pub fn sendResponse(self: *CliClient, response: []const u8) !void {
            try self.stream.writeAll(response);
            try self.stream.writeAll("\n");
        }

        pub fn sendError(self: *CliClient, error_msg: []const u8) !void {
            var buffer: [1024]u8 = undefined;
            const formatted = try std.fmt.bufPrint(&buffer, "ERROR: {s}\n", .{error_msg});
            try self.stream.writeAll(formatted);
        }

        pub fn sendJson(self: *CliClient, allocator: std.mem.Allocator, value: std.json.Value) !void {
            const json_str = try std.json.stringifyAlloc(allocator, value, .{ .whitespace = .indent_2 });
            defer allocator.free(json_str);
            try self.sendResponse(json_str);
        }
    };

    pub fn init(allocator: std.mem.Allocator, config: *const Config) CliServer {
        return CliServer{
            .socket_path = config.cli.socket_path,
            .server = null,
            .running = false,
            .allocator = allocator,
            .clients = std.ArrayList(*CliClient).init(allocator),
            .message_broker = null,
        };
    }

    pub fn deinit(self: *CliServer) void {
        self.stop();

        // Clean up clients
        for (self.clients.items) |client| {
            client.deinit();
            self.allocator.destroy(client);
        }
        self.clients.deinit();
    }

    pub fn setMessageBroker(self: *CliServer, broker: *MessageBroker) void {
        self.message_broker = broker;
    }

    pub fn start(self: *CliServer) !void {
        // Remove existing socket file if it exists
        std.fs.cwd().deleteFile(self.socket_path) catch {};

        const address = try std.net.Address.initUnix(self.socket_path);
        self.server = try address.listen(.{});
        self.running = true;

        std.log.info("CLI server listening on {s}", .{self.socket_path});

        // Accept connections
        while (self.running) {
            const client_socket = self.server.?.accept() catch |err| {
                std.log.err("Failed to accept CLI connection: {}", .{err});
                continue;
            };

            // Handle client in separate thread (simplified for now)
            self.handleClient(client_socket) catch |err| {
                std.log.err("Failed to handle CLI client: {}", .{err});
                client_socket.stream.close();
            };
        }
    }

    pub fn stop(self: *CliServer) void {
        self.running = false;
        if (self.server) |*server| {
            server.deinit();
            self.server = null;
        }

        // Remove socket file
        std.fs.cwd().deleteFile(self.socket_path) catch {};
    }

    fn handleClient(self: *CliServer, client_socket: std.net.Server.Connection) !void {
        const client_id = @as(u64, @intCast(self.clients.items.len + 1));
        const client = try self.allocator.create(CliClient);
        client.* = CliClient.init(self.allocator, client_id, client_socket.stream);

        try self.clients.append(client);
        defer self.removeClient(client);

        try client.sendResponse("Welcome to Yak CLI! Type 'help' for available commands.");

        var buffer: [4096]u8 = undefined;
        while (self.running) {
            const bytes_read = client.stream.read(&buffer) catch |err| {
                std.log.debug("CLI client {} disconnected: {}", .{ client.id, err });
                break;
            };

            if (bytes_read == 0) break;

            const command_line = std.mem.trim(u8, buffer[0..bytes_read], " \t\n\r");
            if (command_line.len == 0) continue;

            self.processCommand(client, command_line) catch |err| {
                std.log.err("Command processing error: {}", .{err});
                try client.sendError("Internal server error");
            };
        }
    }

    fn removeClient(self: *CliServer, client: *CliClient) void {
        for (self.clients.items, 0..) |c, i| {
            if (c.id == client.id) {
                _ = self.clients.swapRemove(i);
                break;
            }
        }
        client.deinit();
        self.allocator.destroy(client);
    }

    fn processCommand(self: *CliServer, client: *CliClient, command_line: []const u8) !void {
        var args = std.mem.split(u8, command_line, " ");
        const command = args.next() orelse return;

        if (std.mem.eql(u8, command, "help")) {
            try self.handleHelp(client);
        } else if (std.mem.eql(u8, command, "quit") or std.mem.eql(u8, command, "exit")) {
            try client.sendResponse("Goodbye!");
        } else if (std.mem.eql(u8, command, "status")) {
            try self.handleStatus(client);
        } else if (std.mem.eql(u8, command, "stats")) {
            try self.handleStats(client);
        } else if (std.mem.eql(u8, command, "queue")) {
            try self.handleQueueCommand(client, &args);
        } else if (std.mem.eql(u8, command, "exchange")) {
            try self.handleExchangeCommand(client, &args);
        } else if (std.mem.eql(u8, command, "connection")) {
            try self.handleConnectionCommand(client, &args);
        } else if (std.mem.eql(u8, command, "vhost")) {
            try self.handleVhostCommand(client, &args);
        } else {
            try client.sendError("Unknown command. Type 'help' for available commands.");
        }
    }

    fn handleHelp(self: *CliServer, client: *CliClient) !void {
        _ = self;
        const help_text =
            \\Available commands:
            \\
            \\  help                     Show this help message
            \\  status                   Show broker status
            \\  stats                    Show detailed statistics
            \\  quit/exit               Disconnect from CLI
            \\
            \\  queue list              List all queues
            \\  queue info <name>       Show queue information
            \\  queue declare <name>    Declare a new queue
            \\  queue delete <name>     Delete a queue
            \\  queue purge <name>      Purge messages from queue
            \\
            \\  exchange list           List all exchanges
            \\  exchange info <name>    Show exchange information
            \\  exchange declare <name> <type>  Declare exchange
            \\  exchange delete <name>  Delete an exchange
            \\
            \\  connection list         List active connections
            \\  connection info <id>    Show connection details
            \\  connection close <id>   Close a connection
            \\
            \\  vhost list              List virtual hosts
            \\  vhost info <name>       Show virtual host details
            \\
        ;
        try client.sendResponse(help_text);
    }

    fn handleStatus(self: *CliServer, client: *CliClient) !void {
        if (self.message_broker) |broker| {
            const stats = try broker.getStats(self.allocator);
            defer stats.object.deinit();

            try client.sendJson(self.allocator, stats);
        } else {
            try client.sendError("Message broker not available");
        }
    }

    fn handleStats(self: *CliServer, client: *CliClient) !void {
        if (self.message_broker) |broker| {
            var detailed_stats = std.json.ObjectMap.init(self.allocator);
            defer detailed_stats.deinit();

            // Get broker stats
            const broker_stats = try broker.getStats(self.allocator);
            defer broker_stats.object.deinit();
            try detailed_stats.put("broker", broker_stats);

            // Add system stats
            var system_stats = std.json.ObjectMap.init(self.allocator);
            try system_stats.put("uptime_seconds", std.json.Value{ .integer = @intCast(std.time.timestamp()) });
            try system_stats.put("cli_clients", std.json.Value{ .integer = @intCast(self.clients.items.len) });
            try detailed_stats.put("system", std.json.Value{ .object = system_stats });

            const stats_value = std.json.Value{ .object = detailed_stats };
            try client.sendJson(self.allocator, stats_value);
        } else {
            try client.sendError("Message broker not available");
        }
    }

    fn handleQueueCommand(self: *CliServer, client: *CliClient, args: *std.mem.SplitIterator(u8, .scalar)) !void {
        const subcommand = args.next() orelse {
            try client.sendError("Queue subcommand required. Use 'help' for usage.");
            return;
        };

        if (std.mem.eql(u8, subcommand, "list")) {
            try self.handleQueueList(client);
        } else if (std.mem.eql(u8, subcommand, "info")) {
            const queue_name = args.next() orelse {
                try client.sendError("Queue name required");
                return;
            };
            try self.handleQueueInfo(client, queue_name);
        } else if (std.mem.eql(u8, subcommand, "declare")) {
            const queue_name = args.next() orelse {
                try client.sendError("Queue name required");
                return;
            };
            try self.handleQueueDeclare(client, queue_name);
        } else if (std.mem.eql(u8, subcommand, "delete")) {
            const queue_name = args.next() orelse {
                try client.sendError("Queue name required");
                return;
            };
            try self.handleQueueDelete(client, queue_name);
        } else if (std.mem.eql(u8, subcommand, "purge")) {
            const queue_name = args.next() orelse {
                try client.sendError("Queue name required");
                return;
            };
            try self.handleQueuePurge(client, queue_name);
        } else {
            try client.sendError("Unknown queue subcommand");
        }
    }

    fn handleQueueList(self: *CliServer, client: *CliClient) !void {
        _ = self;
        // TODO: Implement queue listing with actual broker integration
        try client.sendResponse("Queue listing not yet implemented");
    }

    fn handleQueueInfo(self: *CliServer, client: *CliClient, queue_name: []const u8) !void {
        _ = self;
        _ = queue_name;
        // TODO: Implement queue info with actual broker integration
        try client.sendResponse("Queue info not yet implemented");
    }

    fn handleQueueDeclare(self: *CliServer, client: *CliClient, queue_name: []const u8) !void {
        _ = self;
        _ = queue_name;
        // TODO: Implement queue declaration with actual broker integration
        try client.sendResponse("Queue declaration not yet implemented");
    }

    fn handleQueueDelete(self: *CliServer, client: *CliClient, queue_name: []const u8) !void {
        _ = self;
        _ = queue_name;
        // TODO: Implement queue deletion with actual broker integration
        try client.sendResponse("Queue deletion not yet implemented");
    }

    fn handleQueuePurge(self: *CliServer, client: *CliClient, queue_name: []const u8) !void {
        _ = self;
        _ = queue_name;
        // TODO: Implement queue purging with actual broker integration
        try client.sendResponse("Queue purging not yet implemented");
    }

    fn handleExchangeCommand(self: *CliServer, client: *CliClient, args: *std.mem.SplitIterator(u8, .scalar)) !void {
        _ = self;
        _ = args;
        // TODO: Implement exchange commands
        try client.sendResponse("Exchange commands not yet implemented");
    }

    fn handleConnectionCommand(self: *CliServer, client: *CliClient, args: *std.mem.SplitIterator(u8, .scalar)) !void {
        _ = self;
        _ = args;
        // TODO: Implement connection commands
        try client.sendResponse("Connection commands not yet implemented");
    }

    fn handleVhostCommand(self: *CliServer, client: *CliClient, args: *std.mem.SplitIterator(u8, .scalar)) !void {
        _ = self;
        _ = args;
        // TODO: Implement vhost commands
        try client.sendResponse("Virtual host commands not yet implemented");
    }
};

test "cli server basic operations" {
    const allocator = std.testing.allocator;

    const config = @import("../config.zig").Config{
        .tcp = undefined,
        .storage = undefined,
        .limits = undefined,
        .auth = undefined,
        .cli = .{
            .enabled = true,
            .socket_path = "/tmp/test-yak-cli.sock",
            .timeout = 30,
            .require_auth = false,
            .max_connections = 10,
        },
    };

    var cli_server = CliServer.init(allocator, &config);
    defer cli_server.deinit();

    try std.testing.expectEqual(false, cli_server.running);
    try std.testing.expectEqual(@as(usize, 0), cli_server.clients.items.len);
}
