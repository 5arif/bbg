const std = @import("std");
const kafka = @import("kafka");
const order_event = @import("../events/order_event.zig");
const OrderEvent = order_event.OrderEvent;

const log = std.log.scoped(.order_consumer);

pub const OrderConsumer = struct {
    allocator: std.mem.Allocator,
    consumer: kafka.Consumer,

    pub fn init(allocator: std.mem.Allocator, bootstrap_servers: [:0]const u8, group_id: [:0]const u8) !OrderConsumer {
        const config = kafka.ConfigBuilder.get()
            .with("bootstrap.servers", bootstrap_servers.ptr)
            .with("group.id", group_id.ptr)
            .with("auto.offset.reset", "earliest")
            // -- Performance Tuning --
            .with("fetch.min.bytes", "1048576") // Fetch at least 1MB at a time
            .with("fetch.wait.max.ms", "100") // Wait up to 100ms if not enough data
            .with("fetch.message.max.bytes", "10000000") // 10MB max message size per partition
            .with("socket.receive.buffer.bytes", "1048576") // 1MB socket receive buffer
            .build();

        const consumer = kafka.Consumer.init(config);

        // Basic check if consumer initialization succeeded
        // The library might set internal state to null on failure
        if (consumer._consumer == null) {
            return error.KafkaInitializationFailed;
        }

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

        log.info("Started consuming from topic: {s} (Optimized mode)", .{topic});

        // Use a single arena outside the loop for all per-message allocations.
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        while (true) {
            // poll for a message
            const message = self.consumer.poll(1000) orelse continue;
            defer message.deinit();

            const payload = message.getPayload();
            if (payload.len == 0) continue;

            // Reset arena while retaining capacity.
            // This prevents the allocator from repeatedly requesting memory from the OS.
            _ = arena.reset(.retain_capacity);
            const arena_allocator = arena.allocator();

            const parsed = std.json.parseFromSlice(OrderEvent, arena_allocator, payload, .{
                .ignore_unknown_fields = true,
            }) catch |err| {
                log.err("Failed to parse message: {s}. Error: {any}", .{ payload, err });
                continue;
            };

            const event = parsed.value;
            // Only log if necessary, logging can be a major bottleneck for performance.
            // For now we keep it as requested, but reduced verbosity might be better for pure speed.
            log.info("Received OrderEvent: OrderId={s}", .{event.OrderId});
        }
    }
};
