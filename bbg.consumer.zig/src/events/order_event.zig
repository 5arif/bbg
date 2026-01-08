const std = @import("std");

pub const OrderEvent = struct {
    OrderId: []const u8,
    Amount: f64,
    Customer: []const u8,
};
