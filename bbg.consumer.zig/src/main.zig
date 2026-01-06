const std = @import("std");
const bbg_consumer_zig = @import("bbg_consumer_zig");
const OrderConsumer = bbg_consumer_zig.OrderConsumer;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    const bootstrap_servers = "localhost:9092";
    const group_id = "order_consumer_group";
    const topic = "orders.events";

    var consumer = try OrderConsumer.init(allocator, bootstrap_servers, group_id);
    defer consumer.deinit();

    try consumer.consume(topic);
}
