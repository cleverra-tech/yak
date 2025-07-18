const std = @import("std");
const Config = @import("../config.zig").Config;

pub const AuthResult = enum {
    success,
    invalid_credentials,
    account_disabled,
    rate_limited,
    system_error,
};

pub const User = struct {
    username: []const u8,
    password_hash: []const u8,
    algorithm: PasswordHashAlgorithm,
    enabled: bool,
    permissions: Permissions,
    created_at: i64,
    last_login: ?i64,
    login_attempts: u32,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, username: []const u8, password: []const u8, algorithm: PasswordHashAlgorithm) !User {
        const owned_username = try allocator.dupe(u8, username);
        const password_hash = try hashPassword(allocator, password, algorithm);

        return User{
            .username = owned_username,
            .password_hash = password_hash,
            .algorithm = algorithm,
            .enabled = true,
            .permissions = Permissions.init(),
            .created_at = std.time.timestamp(),
            .last_login = null,
            .login_attempts = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *User) void {
        self.allocator.free(self.username);
        self.allocator.free(self.password_hash);
    }

    pub fn verifyPassword(self: *const User, password: []const u8) !bool {
        const computed_hash = try hashPassword(self.allocator, password, self.algorithm);
        defer self.allocator.free(computed_hash);

        return std.mem.eql(u8, self.password_hash, computed_hash);
    }

    pub fn updatePassword(self: *User, new_password: []const u8) !void {
        const new_hash = try hashPassword(self.allocator, new_password, self.algorithm);
        self.allocator.free(self.password_hash);
        self.password_hash = new_hash;
    }

    pub fn recordLogin(self: *User, successful: bool) void {
        if (successful) {
            self.last_login = std.time.timestamp();
            self.login_attempts = 0;
        } else {
            self.login_attempts += 1;
        }
    }

    pub fn isLocked(self: *const User) bool {
        return self.login_attempts >= 5; // Lock after 5 failed attempts
    }
};

pub const PasswordHashAlgorithm = enum {
    plain,
    sha256,
    argon2,

    pub fn fromConfig(config_algo: Config.AuthConfig.PasswordHashAlgorithm) PasswordHashAlgorithm {
        return switch (config_algo) {
            .plain => .plain,
            .sha256 => .sha256,
            .argon2 => .argon2,
        };
    }
};

pub const Permissions = struct {
    can_publish: bool,
    can_consume: bool,
    can_declare_exchange: bool,
    can_delete_exchange: bool,
    can_declare_queue: bool,
    can_delete_queue: bool,
    can_bind: bool,
    can_unbind: bool,
    can_purge: bool,
    allowed_vhosts: std.ArrayList([]const u8),

    pub fn init() Permissions {
        return Permissions{
            .can_publish = true,
            .can_consume = true,
            .can_declare_exchange = true,
            .can_delete_exchange = true,
            .can_declare_queue = true,
            .can_delete_queue = true,
            .can_bind = true,
            .can_unbind = true,
            .can_purge = true,
            .allowed_vhosts = std.ArrayList([]const u8).init(std.heap.page_allocator),
        };
    }

    pub fn deinit(self: *Permissions) void {
        for (self.allowed_vhosts.items) |vhost| {
            std.heap.page_allocator.free(vhost);
        }
        self.allowed_vhosts.deinit();
    }

    pub fn allowVhost(self: *Permissions, vhost: []const u8) !void {
        const owned_vhost = try std.heap.page_allocator.dupe(u8, vhost);
        try self.allowed_vhosts.append(owned_vhost);
    }

    pub fn canAccessVhost(self: *const Permissions, vhost: []const u8) bool {
        if (self.allowed_vhosts.items.len == 0) {
            return true; // No restrictions
        }

        for (self.allowed_vhosts.items) |allowed_vhost| {
            if (std.mem.eql(u8, allowed_vhost, vhost)) {
                return true;
            }
        }

        return false;
    }
};

pub const AuthManager = struct {
    users: std.HashMap([]const u8, *User, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    default_algorithm: PasswordHashAlgorithm,
    rate_limiter: RateLimiter,
    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, config: *const Config) !AuthManager {
        var auth_manager = AuthManager{
            .users = std.HashMap([]const u8, *User, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .default_algorithm = PasswordHashAlgorithm.fromConfig(config.auth.password_hash_algorithm),
            .rate_limiter = RateLimiter.init(allocator),
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
        };

        // Create default user
        try auth_manager.createUser(config.auth.default_user, config.auth.default_password);

        std.log.info("Authentication manager initialized with default user: {s}", .{config.auth.default_user});

        return auth_manager;
    }

    pub fn deinit(self: *AuthManager) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var iterator = self.users.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.users.deinit();
        self.rate_limiter.deinit();
    }

    pub fn createUser(self: *AuthManager, username: []const u8, password: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.users.contains(username)) {
            return error.UserAlreadyExists;
        }

        const user = try self.allocator.create(User);
        user.* = try User.init(self.allocator, username, password, self.default_algorithm);

        const owned_username = try self.allocator.dupe(u8, username);
        try self.users.put(owned_username, user);

        std.log.info("User created: {s}", .{username});
    }

    pub fn deleteUser(self: *AuthManager, username: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.users.fetchRemove(username)) |entry| {
            entry.value.deinit();
            self.allocator.destroy(entry.value);
            self.allocator.free(entry.key);
            std.log.info("User deleted: {s}", .{username});
        } else {
            return error.UserNotFound;
        }
    }

    pub fn authenticate(self: *AuthManager, username: []const u8, password: []const u8, client_ip: ?[]const u8) !AuthResult {
        // Check rate limiting
        if (client_ip) |ip| {
            if (!self.rate_limiter.allowRequest(ip)) {
                std.log.warn("Rate limit exceeded for IP: {s}", .{ip});
                return .rate_limited;
            }
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        const user = self.users.get(username) orelse {
            std.log.warn("Authentication failed: user not found: {s}", .{username});
            return .invalid_credentials;
        };

        if (!user.enabled) {
            std.log.warn("Authentication failed: account disabled: {s}", .{username});
            return .account_disabled;
        }

        if (user.isLocked()) {
            std.log.warn("Authentication failed: account locked: {s}", .{username});
            return .account_disabled;
        }

        const password_valid = user.verifyPassword(password) catch |err| {
            std.log.err("Password verification error for user {s}: {}", .{ username, err });
            return .system_error;
        };

        user.recordLogin(password_valid);

        if (password_valid) {
            std.log.info("Authentication successful: {s}", .{username});
            return .success;
        } else {
            std.log.warn("Authentication failed: invalid password: {s}", .{username});
            return .invalid_credentials;
        }
    }

    pub fn getUser(self: *AuthManager, username: []const u8) ?*User {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.users.get(username);
    }

    pub fn changePassword(self: *AuthManager, username: []const u8, old_password: []const u8, new_password: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const user = self.users.get(username) orelse return error.UserNotFound;

        const old_password_valid = try user.verifyPassword(old_password);
        if (!old_password_valid) {
            return error.InvalidCredentials;
        }

        try user.updatePassword(new_password);
        std.log.info("Password changed for user: {s}", .{username});
    }

    pub fn enableUser(self: *AuthManager, username: []const u8, enabled: bool) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const user = self.users.get(username) orelse return error.UserNotFound;
        user.enabled = enabled;

        std.log.info("User {s} {s}", .{ username, if (enabled) "enabled" else "disabled" });
    }

    pub fn getUserCount(self: *const AuthManager) u32 {
        return @intCast(self.users.count());
    }

    pub fn getStats(self: *const AuthManager, allocator: std.mem.Allocator) !std.json.Value {
        var stats = std.json.ObjectMap.init(allocator);

        try stats.put("total_users", std.json.Value{ .integer = @intCast(self.users.count()) });

        var enabled_count: u32 = 0;
        var locked_count: u32 = 0;

        var iterator = self.users.valueIterator();
        while (iterator.next()) |user| {
            if (user.*.enabled) enabled_count += 1;
            if (user.*.isLocked()) locked_count += 1;
        }

        try stats.put("enabled_users", std.json.Value{ .integer = @intCast(enabled_count) });
        try stats.put("locked_users", std.json.Value{ .integer = @intCast(locked_count) });
        try stats.put("algorithm", std.json.Value{ .string = @tagName(self.default_algorithm) });

        return std.json.Value{ .object = stats };
    }
};

const RateLimiter = struct {
    requests: std.HashMap([]const u8, RequestInfo, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    allocator: std.mem.Allocator,

    const RequestInfo = struct {
        count: u32,
        window_start: i64,
    };

    const WINDOW_SIZE_SECONDS = 60;
    const MAX_REQUESTS_PER_WINDOW = 10;

    pub fn init(allocator: std.mem.Allocator) RateLimiter {
        return RateLimiter{
            .requests = std.HashMap([]const u8, RequestInfo, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RateLimiter) void {
        var iterator = self.requests.iterator();
        while (iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.requests.deinit();
    }

    pub fn allowRequest(self: *RateLimiter, client_ip: []const u8) bool {
        const now = std.time.timestamp();

        const gop = self.requests.getOrPut(client_ip) catch return false;
        if (!gop.found_existing) {
            gop.key_ptr.* = self.allocator.dupe(u8, client_ip) catch return false;
            gop.value_ptr.* = RequestInfo{
                .count = 1,
                .window_start = now,
            };
            return true;
        }

        const info = gop.value_ptr;

        // Check if we need a new window
        if (now - info.window_start >= WINDOW_SIZE_SECONDS) {
            info.count = 1;
            info.window_start = now;
            return true;
        }

        // Check if we've exceeded the limit
        if (info.count >= MAX_REQUESTS_PER_WINDOW) {
            return false;
        }

        info.count += 1;
        return true;
    }
};

fn hashPassword(allocator: std.mem.Allocator, password: []const u8, algorithm: PasswordHashAlgorithm) ![]u8 {
    switch (algorithm) {
        .plain => {
            return try allocator.dupe(u8, password);
        },
        .sha256 => {
            var hasher = std.crypto.hash.sha2.Sha256.init(.{});
            hasher.update(password);
            var hash: [32]u8 = undefined;
            hasher.final(&hash);

            return try std.fmt.allocPrint(allocator, "{x}", .{std.fmt.fmtSliceHexLower(&hash)});
        },
        .argon2 => {
            // For production, use proper Argon2 implementation
            // This is a simplified version for demonstration
            var hasher = std.crypto.hash.sha2.Sha256.init(.{});
            hasher.update("argon2_salt_");
            hasher.update(password);
            var hash: [32]u8 = undefined;
            hasher.final(&hash);

            return try std.fmt.allocPrint(allocator, "argon2:{x}", .{std.fmt.fmtSliceHexLower(&hash)});
        },
    }
}

test "user creation and authentication" {
    const allocator = std.testing.allocator;

    var user = try User.init(allocator, "testuser", "testpass", .sha256);
    defer user.deinit();

    try std.testing.expectEqualStrings("testuser", user.username);
    try std.testing.expectEqual(true, user.enabled);
    try std.testing.expectEqual(@as(u32, 0), user.login_attempts);

    // Test password verification
    const valid = try user.verifyPassword("testpass");
    try std.testing.expectEqual(true, valid);

    const invalid = try user.verifyPassword("wrongpass");
    try std.testing.expectEqual(false, invalid);
}

test "auth manager operations" {
    const allocator = std.testing.allocator;

    const config = Config{
        .tcp = undefined,
        .storage = undefined,
        .limits = undefined,
        .auth = .{
            .default_user = "admin",
            .default_password = "admin123",
            .password_hash_algorithm = .sha256,
        },
        .cli = undefined,
    };

    var auth_manager = try AuthManager.init(allocator, &config);
    defer auth_manager.deinit();

    try std.testing.expectEqual(@as(u32, 1), auth_manager.getUserCount());

    // Test authentication
    const result = try auth_manager.authenticate("admin", "admin123", null);
    try std.testing.expectEqual(AuthResult.success, result);

    const invalid_result = try auth_manager.authenticate("admin", "wrongpass", null);
    try std.testing.expectEqual(AuthResult.invalid_credentials, invalid_result);

    // Test user creation
    try auth_manager.createUser("newuser", "newpass");
    try std.testing.expectEqual(@as(u32, 2), auth_manager.getUserCount());
}
