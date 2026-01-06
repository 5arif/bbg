const std = @import("std");
const kafka = @import("kafka");
const order_event = @import("../events/order_event.zig");
const OrderEvent = order_event.OrderEvent;

pub const OrderConsumer = struct {
    allocator: std.mem.Allocator,
    consumer: kafka.Consumer,

    pub fn init(allocator: std.mem.Allocator, bootstrap_servers: [:0]const u8, group_id: [:0]const u8) !OrderConsumer {
        const config = kafka.ConfigBuilder.get()
            .with("bootstrap.servers", bootstrap_servers.ptr)
            .with("group.id", group_id.ptr)
            .with("auto.offset.reset", "earliest")
            .build();

        const consumer = kafka.Consumer.init(config);

        return OrderConsumer{
            .allocator = allocator,
            .consumer = consumer,
        };
    }

    pub fn deinit(self: *OrderConsumer) void {
        self.consumer.deinit();
    }

    pub fn consume(self: *OrderConsumer, topic: []const u8) !void {
        const topics = [_][]const u8{topic};
        self.consumer.subscribe(&topics);

        std.debug.print("Started consuming from topic: {s}\n", .{topic});

        while (true) {
            if (self.consumer.poll(1000)) |message| {
                defer message.deinit();

                const payload = message.getPayload();
                if (payload.len == 0) continue;

                var parsed = std.json.parseFromSlice(OrderEvent, self.allocator, payload, .{
                    .ignore_unknown_fields = true,
                }) catch |err| {
                    std.debug.print("Failed to parse message: {s}. Error: {}\n", .{ payload, err });
                    continue;
                };
                defer parsed.deinit();

                const event = parsed.value;
                std.debug.print("Received OrderEvent: OrderId={s}, Amount={d:.2}, Customer={s}\n", .{
                    event.OrderId,
                    event.Amount,
                    event.Customer,
                });
            }
        }
    }
};
