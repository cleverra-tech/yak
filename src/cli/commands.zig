const std = @import("std");

/// Command result types
pub const CommandResult = union(enum) {
    success: []const u8,
    error_msg: []const u8,
    json: std.json.Value,
};

/// Base command interface
pub const Command = struct {
    name: []const u8,
    description: []const u8,
    execute: *const fn (allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) anyerror!CommandResult,

    pub fn run(self: *const Command, allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
        return try self.execute(allocator, args, context);
    }
};

/// Context passed to commands for accessing broker state
pub const CommandContext = struct {
    broker: ?*@import("../core/server.zig").Server,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) CommandContext {
        return CommandContext{
            .broker = null,
            .allocator = allocator,
        };
    }

    pub fn setBroker(self: *CommandContext, broker: *@import("../core/server.zig").Server) void {
        self.broker = broker;
    }
};

/// Command registry
pub const CommandRegistry = struct {
    commands: std.HashMap([]const u8, Command, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) CommandRegistry {
        var registry = CommandRegistry{
            .commands = std.HashMap([]const u8, Command, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .allocator = allocator,
        };

        // Register built-in commands
        registry.registerBuiltinCommands() catch |err| {
            std.log.err("Failed to register builtin commands: {}", .{err});
        };

        return registry;
    }

    pub fn deinit(self: *CommandRegistry) void {
        var iterator = self.commands.iterator();
        while (iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.commands.deinit();
    }

    pub fn register(self: *CommandRegistry, command: Command) !void {
        const owned_name = try self.allocator.dupe(u8, command.name);
        try self.commands.put(owned_name, command);
    }

    pub fn get(self: *CommandRegistry, name: []const u8) ?Command {
        return self.commands.get(name);
    }

    pub fn list(self: *CommandRegistry, allocator: std.mem.Allocator) ![]Command {
        var command_list = std.ArrayList(Command).init(allocator);

        var iterator = self.commands.valueIterator();
        while (iterator.next()) |command| {
            try command_list.append(command.*);
        }

        return command_list.toOwnedSlice();
    }

    fn registerBuiltinCommands(self: *CommandRegistry) !void {
        // Status commands
        try self.register(Command{
            .name = "status",
            .description = "Show broker status",
            .execute = statusCommand,
        });

        try self.register(Command{
            .name = "stats",
            .description = "Show detailed statistics",
            .execute = statsCommand,
        });

        try self.register(Command{
            .name = "health",
            .description = "Show health check information",
            .execute = healthCommand,
        });

        // Queue commands
        try self.register(Command{
            .name = "queue.list",
            .description = "List all queues",
            .execute = queueListCommand,
        });

        try self.register(Command{
            .name = "queue.info",
            .description = "Show queue information",
            .execute = queueInfoCommand,
        });

        try self.register(Command{
            .name = "queue.declare",
            .description = "Declare a new queue",
            .execute = queueDeclareCommand,
        });

        try self.register(Command{
            .name = "queue.delete",
            .description = "Delete a queue",
            .execute = queueDeleteCommand,
        });

        try self.register(Command{
            .name = "queue.purge",
            .description = "Purge messages from a queue",
            .execute = queuePurgeCommand,
        });

        // Exchange commands
        try self.register(Command{
            .name = "exchange.list",
            .description = "List all exchanges",
            .execute = exchangeListCommand,
        });

        try self.register(Command{
            .name = "exchange.info",
            .description = "Show exchange information",
            .execute = exchangeInfoCommand,
        });

        try self.register(Command{
            .name = "exchange.declare",
            .description = "Declare a new exchange",
            .execute = exchangeDeclareCommand,
        });

        try self.register(Command{
            .name = "exchange.delete",
            .description = "Delete an exchange",
            .execute = exchangeDeleteCommand,
        });

        // Connection commands
        try self.register(Command{
            .name = "connection.list",
            .description = "List active connections",
            .execute = connectionListCommand,
        });

        try self.register(Command{
            .name = "connection.info",
            .description = "Show connection information",
            .execute = connectionInfoCommand,
        });

        try self.register(Command{
            .name = "connection.close",
            .description = "Close a connection",
            .execute = connectionCloseCommand,
        });

        // Virtual host commands
        try self.register(Command{
            .name = "vhost.list",
            .description = "List virtual hosts",
            .execute = vhostListCommand,
        });

        try self.register(Command{
            .name = "vhost.info",
            .description = "Show virtual host information",
            .execute = vhostInfoCommand,
        });
    }
};

// Command implementations

fn statusCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = args;

    if (context.broker) |broker| {
        const stats = try broker.getStats(allocator);
        return CommandResult{ .json = stats };
    } else {
        return CommandResult{ .error_msg = "Message broker not available" };
    }
}

fn statsCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = args;

    if (context.broker) |broker| {
        var detailed_stats = std.json.ObjectMap.init(allocator);

        // Get broker stats
        const broker_stats = try broker.getStats(allocator);
        try detailed_stats.put("broker", broker_stats);

        // Add system information
        var system_info = std.json.ObjectMap.init(allocator);
        try system_info.put("uptime", std.json.Value{ .integer = @intCast(std.time.timestamp()) });
        try system_info.put("zig_version", std.json.Value{ .string = @import("builtin").zig_version_string });
        try system_info.put("os", std.json.Value{ .string = @tagName(@import("builtin").os.tag) });
        try system_info.put("arch", std.json.Value{ .string = @tagName(@import("builtin").cpu.arch) });
        try detailed_stats.put("system", std.json.Value{ .object = system_info });

        return CommandResult{ .json = std.json.Value{ .object = detailed_stats } };
    } else {
        return CommandResult{ .error_msg = "Message broker not available" };
    }
}

fn healthCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = args;

    var health = std.json.ObjectMap.init(allocator);

    if (context.broker) |broker| {
        try health.put("status", std.json.Value{ .string = "healthy" });
        try health.put("connections", std.json.Value{ .integer = @intCast(broker.getConnectionCount()) });
        try health.put("virtual_hosts", std.json.Value{ .integer = @intCast(broker.getVirtualHostCount()) });
    } else {
        try health.put("status", std.json.Value{ .string = "unhealthy" });
        try health.put("reason", std.json.Value{ .string = "broker not available" });
    }

    return CommandResult{ .json = std.json.Value{ .object = health } };
}

fn queueListCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = args;
    _ = allocator;
    _ = context;

    // TODO: Implement actual queue listing
    return CommandResult{ .success = "Queue listing not yet implemented" };
}

fn queueInfoCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = allocator;
    _ = context;

    if (args.len < 1) {
        return CommandResult{ .error_msg = "Queue name required" };
    }

    const queue_name = args[0];
    _ = queue_name;

    // TODO: Implement actual queue info retrieval
    return CommandResult{ .success = "Queue info not yet implemented" };
}

fn queueDeclareCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = allocator;
    _ = context;

    if (args.len < 1) {
        return CommandResult{ .error_msg = "Queue name required" };
    }

    const queue_name = args[0];
    _ = queue_name;

    // TODO: Implement actual queue declaration
    return CommandResult{ .success = "Queue declaration not yet implemented" };
}

fn queueDeleteCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = allocator;
    _ = context;

    if (args.len < 1) {
        return CommandResult{ .error_msg = "Queue name required" };
    }

    const queue_name = args[0];
    _ = queue_name;

    // TODO: Implement actual queue deletion
    return CommandResult{ .success = "Queue deletion not yet implemented" };
}

fn queuePurgeCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = allocator;
    _ = context;

    if (args.len < 1) {
        return CommandResult{ .error_msg = "Queue name required" };
    }

    const queue_name = args[0];
    _ = queue_name;

    // TODO: Implement actual queue purging
    return CommandResult{ .success = "Queue purging not yet implemented" };
}

fn exchangeListCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = args;
    _ = allocator;
    _ = context;

    // TODO: Implement actual exchange listing
    return CommandResult{ .success = "Exchange listing not yet implemented" };
}

fn exchangeInfoCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = allocator;
    _ = context;

    if (args.len < 1) {
        return CommandResult{ .error_msg = "Exchange name required" };
    }

    const exchange_name = args[0];
    _ = exchange_name;

    // TODO: Implement actual exchange info retrieval
    return CommandResult{ .success = "Exchange info not yet implemented" };
}

fn exchangeDeclareCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = allocator;
    _ = context;

    if (args.len < 2) {
        return CommandResult{ .error_msg = "Exchange name and type required" };
    }

    const exchange_name = args[0];
    const exchange_type = args[1];
    _ = exchange_name;
    _ = exchange_type;

    // TODO: Implement actual exchange declaration
    return CommandResult{ .success = "Exchange declaration not yet implemented" };
}

fn exchangeDeleteCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = allocator;
    _ = context;

    if (args.len < 1) {
        return CommandResult{ .error_msg = "Exchange name required" };
    }

    const exchange_name = args[0];
    _ = exchange_name;

    // TODO: Implement actual exchange deletion
    return CommandResult{ .success = "Exchange deletion not yet implemented" };
}

fn connectionListCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = args;
    _ = allocator;
    _ = context;

    // TODO: Implement actual connection listing
    return CommandResult{ .success = "Connection listing not yet implemented" };
}

fn connectionInfoCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = allocator;
    _ = context;

    if (args.len < 1) {
        return CommandResult{ .error_msg = "Connection ID required" };
    }

    const connection_id = args[0];
    _ = connection_id;

    // TODO: Implement actual connection info retrieval
    return CommandResult{ .success = "Connection info not yet implemented" };
}

fn connectionCloseCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = allocator;
    _ = context;

    if (args.len < 1) {
        return CommandResult{ .error_msg = "Connection ID required" };
    }

    const connection_id = args[0];
    _ = connection_id;

    // TODO: Implement actual connection closing
    return CommandResult{ .success = "Connection closing not yet implemented" };
}

fn vhostListCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = args;
    _ = allocator;
    _ = context;

    // TODO: Implement actual vhost listing
    return CommandResult{ .success = "Virtual host listing not yet implemented" };
}

fn vhostInfoCommand(allocator: std.mem.Allocator, args: []const []const u8, context: *CommandContext) !CommandResult {
    _ = allocator;
    _ = context;

    if (args.len < 1) {
        return CommandResult{ .error_msg = "Virtual host name required" };
    }

    const vhost_name = args[0];
    _ = vhost_name;

    // TODO: Implement actual vhost info retrieval
    return CommandResult{ .success = "Virtual host info not yet implemented" };
}

test "command registry operations" {
    const allocator = std.testing.allocator;

    var registry = CommandRegistry.init(allocator);
    defer registry.deinit();

    // Test getting a built-in command
    const status_cmd = registry.get("status");
    try std.testing.expect(status_cmd != null);
    try std.testing.expectEqualStrings("status", status_cmd.?.name);

    // Test listing commands
    const commands = try registry.list(allocator);
    defer allocator.free(commands);
    try std.testing.expect(commands.len > 0);
}

test "status command execution" {
    const allocator = std.testing.allocator;

    var context = CommandContext.init(allocator);

    const result = try statusCommand(allocator, &[_][]const u8{}, &context);

    switch (result) {
        .error_msg => |msg| {
            try std.testing.expectEqualStrings("Message broker not available", msg);
        },
        else => try std.testing.expect(false), // Should be an error since no broker is set
    }
}
