const std = @import("std");

/// AMQP field table parser and encoder for protocol property tables
pub const FieldTable = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) FieldTable {
        return FieldTable{
            .allocator = allocator,
        };
    }

    /// Encode a string-to-string hash map as an AMQP field table
    pub fn encode(self: *FieldTable, table: std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage)) ![]u8 {
        var buffer = std.ArrayList(u8).init(self.allocator);
        defer buffer.deinit();

        var iterator = table.iterator();
        while (iterator.next()) |entry| {
            const key = entry.key_ptr.*;
            const value = entry.value_ptr.*;

            // Write key (short string)
            try buffer.append(@intCast(key.len));
            try buffer.appendSlice(key);

            // Write value type (long string = 'S')
            try buffer.append('S');

            // Write value length and data
            try buffer.appendSlice(&std.mem.toBytes(@as(u32, @intCast(value.len))));
            try buffer.appendSlice(value);
        }

        return buffer.toOwnedSlice();
    }

    /// Parse AMQP field table data into a string-to-string hash map
    pub fn parse(self: *FieldTable, data: []const u8) !std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage) {
        var table = std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(self.allocator);
        var offset: usize = 0;

        while (offset < data.len) {
            // Parse key (short string)
            if (offset >= data.len) break;
            const key_len = data[offset];
            offset += 1;

            if (offset + key_len > data.len) break;
            const key = try self.allocator.dupe(u8, data[offset .. offset + key_len]);
            offset += key_len;

            // Parse value type
            if (offset >= data.len) {
                self.allocator.free(key);
                break;
            }
            const value_type = data[offset];
            offset += 1;

            // Parse value based on type
            switch (value_type) {
                'S' => { // Long string
                    if (offset + 4 > data.len) {
                        self.allocator.free(key);
                        break;
                    }
                    const value_len = std.mem.readInt(u32, data[offset .. offset + 4][0..4], .big);
                    offset += 4;

                    if (offset + value_len > data.len) {
                        self.allocator.free(key);
                        break;
                    }
                    const value = try self.allocator.dupe(u8, data[offset .. offset + value_len]);
                    offset += value_len;

                    try table.put(key, value);
                },
                's' => { // Short string
                    if (offset >= data.len) {
                        self.allocator.free(key);
                        break;
                    }
                    const value_len = data[offset];
                    offset += 1;

                    if (offset + value_len > data.len) {
                        self.allocator.free(key);
                        break;
                    }
                    const value = try self.allocator.dupe(u8, data[offset .. offset + value_len]);
                    offset += value_len;

                    try table.put(key, value);
                },
                't' => { // Boolean (true)
                    const value = try self.allocator.dupe(u8, "true");
                    try table.put(key, value);
                },
                'f' => { // Boolean (false)
                    const value = try self.allocator.dupe(u8, "false");
                    try table.put(key, value);
                },
                'I' => { // Signed 32-bit integer
                    if (offset + 4 > data.len) {
                        self.allocator.free(key);
                        break;
                    }
                    const int_value = std.mem.readInt(i32, data[offset .. offset + 4][0..4], .big);
                    offset += 4;

                    const value = try std.fmt.allocPrint(self.allocator, "{}", .{int_value});
                    try table.put(key, value);
                },
                else => {
                    // Unknown type, skip this field
                    std.log.warn("Unknown field table value type: '{c}' for key: {s}", .{ value_type, key });
                    self.allocator.free(key);
                    break;
                },
            }
        }

        return table;
    }

    /// Free all keys and values in a field table
    pub fn free(self: *FieldTable, table: *std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage)) void {
        var iterator = table.iterator();
        while (iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        table.deinit();
    }
};

test "field table encoding and parsing" {
    const allocator = std.testing.allocator;

    var field_table = FieldTable.init(allocator);

    // Create test table
    var original_table = std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator);
    defer {
        var iter = original_table.iterator();
        while (iter.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.*);
        }
        original_table.deinit();
    }

    try original_table.put(try allocator.dupe(u8, "test_key"), try allocator.dupe(u8, "test_value"));
    try original_table.put(try allocator.dupe(u8, "another"), try allocator.dupe(u8, "data"));

    // Encode
    const encoded = try field_table.encode(original_table);
    defer allocator.free(encoded);

    // Decode
    var decoded_table = try field_table.parse(encoded);
    defer field_table.free(&decoded_table);

    // Verify
    try std.testing.expectEqual(@as(u32, 2), decoded_table.count());
    try std.testing.expect(decoded_table.contains("test_key"));
    try std.testing.expect(decoded_table.contains("another"));
    try std.testing.expectEqualStrings("test_value", decoded_table.get("test_key").?);
    try std.testing.expectEqualStrings("data", decoded_table.get("another").?);
}
