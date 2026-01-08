const std = @import("std");
const bbg_consumer_zig = @import("bbg_consumer_zig");
const OrderConsumer = bbg_consumer_zig.OrderConsumer;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const status = gpa.deinit();
        if (status == .leak) @panic("Memory leak detected");
    }
    const allocator = gpa.allocator();

    const bootstrap_servers = "localhost:9092";
    const group_id = "order_consumer_group";
    const topic = "orders.events";

    var consumer = try OrderConsumer.init(allocator, bootstrap_servers, group_id);
    defer consumer.deinit();

    try consumer.consume(topic);
}
