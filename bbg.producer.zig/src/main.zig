const std = @import("std");
const kafka = @import("kafka");
const httpz = @import("httpz");
const OrderEvent = @import("events/order_event.zig").OrderEvent;

pub fn main() !void {
    // 1. Standard General Purpose Allocator for long-lived application state
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Starting Kafka producer...\n", .{});

    // 2. Setup Kafka
    var producer_config_builder = kafka.ConfigBuilder.get();
    const producer_conf = producer_config_builder
        .with("bootstrap.servers", "localhost:9092")
        .with("enable.idempotence", "true")
        .with("batch.num.messages", "10")
        .with("reconnect.backoff.ms", "1000")
        .with("reconnect.backoff.max.ms", "5000")
        .with("linger.ms", "100")
        .with("delivery.timeout.ms", "1800000")
        .with("batch.size", "16384")
        .build();

    var topic_config_builder = kafka.TopicBuilder.get();
    const topic_conf = topic_config_builder
        .with("acks", "all")
        .build();

    // 3. Initialize Producer
    const kafka_producer = kafka.Producer.init(producer_conf, topic_conf, "orders.events");
    defer kafka_producer.deinit();

    // 4. Setup Web API Handler
    var handler = Handler{
        .producer = kafka_producer,
        .allocator = allocator,
    };

    // 5. Initialize and start the HTTP server
    var server = try httpz.Server(*Handler).init(allocator, .{ .port = 8088 }, &handler);
    defer server.deinit();

    var router = try server.router(.{});
    router.post("/order", Handler.order, .{});

    std.debug.print("Web API listening on http://localhost:8088\n", .{});
    std.debug.print("Endpoint: POST /order\n", .{});

    try server.listen();
}

const Handler = struct {
    producer: kafka.Producer,
    allocator: std.mem.Allocator,

    pub fn order(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        const body = req.body() orelse {
            res.status = 400;
            try res.json(.{ .@"error" = "missing body" }, .{});
            return;
        };

        const parsed = std.json.parseFromSlice(OrderEvent, self.allocator, body, .{ .ignore_unknown_fields = true }) catch |err| {
            std.debug.print("JSON Parse Error: {}\n", .{err});
            res.status = 400;
            try res.json(.{ .@"error" = "invalid json" }, .{});
            return;
        };
        defer parsed.deinit();

        produceOrderEvent(self.allocator, self.producer, parsed.value) catch |err| {
            std.debug.print("Kafka Produce Error: {}\n", .{err});
            res.status = 500;
            try res.json(.{ .@"error" = "internal server error" }, .{});
            return;
        };

        try res.json(.{ .status = "Order event sent successfully!", .orderId = parsed.value.OrderId }, .{});
    }
};

fn produceOrderEvent(base_allocator: std.mem.Allocator, producer: kafka.Producer, order: OrderEvent) !void {
    var arena = std.heap.ArenaAllocator.init(base_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // Serialize to JSON
    const json_payload = try std.fmt.allocPrint(allocator, "{f}", .{std.json.fmt(order, .{})});

    std.debug.print("Sending order event to Kafka: {s}\n", .{json_payload});

    producer.send(json_payload, order.OrderId);
    producer.wait(1000);

    std.debug.print("Order event sent successfully!\n", .{});
}
