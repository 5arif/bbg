const std = @import("std");
const kafka = @import("kafka");
const httpz = @import("httpz");
const OrderEvent = @import("events/order_event.zig").OrderEvent;

// Best Practice: Setup a scoped logger
const log = std.log.scoped(.producer_api);

pub fn main() !void {
    // 1. Standard General Purpose Allocator for long-lived application state
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    log.info("Starting Kafka Web API...", .{});

    // 2. Optimized Kafka Config for High Throughput
    var producer_config_builder = kafka.ConfigBuilder.get();
    const producer_conf = producer_config_builder
        .with("bootstrap.servers", "localhost:9092")
        .with("enable.idempotence", "true")
        // Batching: Wait up to 5ms to group messages (better throughput vs 100ms)
        .with("linger.ms", "5")
        .with("batch.num.messages", "1000")
        // Compression: LZ4 is extremely fast and efficient
        .with("compression.codec", "lz4")
        // Performance: increase internal buffer size
        .with("queue.buffering.max.messages", "100000")
        .build();

    const topic_conf = kafka.TopicBuilder.get()
        .with("acks", "1") // 1 = Faster (leader ack), "all" = Safer but slower
        .build();

    // 3. Initialize Producer
    const kafka_producer = kafka.Producer.init(producer_conf, topic_conf, "orders.events");
    defer kafka_producer.deinit();

    // 4. Setup Web API Handler
    var handler = Handler{
        .producer = kafka_producer,
    };

    // 3. Tuned HTTP Server for performance
    var server = try httpz.Server(*Handler).init(allocator, .{
        .port = 8088,
        .thread_pool = .{
            .count = 4, // Utilize multiple cores
            .buffer_size = 16384,
        },
        .address = "0.0.0.0",
    }, &handler);
    defer server.deinit();

    var router = try server.router(.{});
    router.post("/order", Handler.order, .{});

    log.info("Production-tuned server listening on http://localhost:8088", .{});
    try server.listen();
}

const Handler = struct {
    producer: kafka.Producer,

    pub fn order(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        const body = req.body() orelse {
            res.status = 400;
            return res.json(.{ .@"error" = "missing body" }, .{});
        };

        // Best Practice: Use the allocator provided by the request context (usually an Arena)
        const allocator = res.arena;

        // Fast Validation
        const parsed = std.json.parseFromSlice(OrderEvent, allocator, body, .{ .ignore_unknown_fields = true }) catch {
            res.status = 400;
            return res.json(.{ .@"error" = "invalid json schema" }, .{});
        };

        // CRITICAL PERFORMANCE: We removed producer.wait()
        // This makes the operation non-blocking.
        // The message is queued and rdkafka handles the delivery in the background.
        self.producer.send(body, parsed.value.OrderId);

        // Immediately return 202 Accepted (more standard for async operations)
        res.status = 202;
        try res.json(.{ .status = "Accepted", .orderId = parsed.value.OrderId }, .{});
    }
};
