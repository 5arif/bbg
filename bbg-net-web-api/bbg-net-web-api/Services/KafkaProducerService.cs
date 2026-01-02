using bbg_net_web_api.Models;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace bbg_net_web_api.Services
{
    public class KafkaProducerService
    {
        private readonly IProducer<string, string> _producer;
        private const string TopicName = "orders.events";

        public KafkaProducerService(IConfiguration configuration)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"]
            };
            _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        }

        public async Task ProduceMessageAsync(OrderEvent order)
        {
            var jsonMessage = JsonConvert.SerializeObject(order);
            var message = new Message<string, string>
            {
                Key = order.orderId.ToString(), // Key can be a simple string
                Value = jsonMessage
            };            
            var deliveryReport = await _producer.ProduceAsync(TopicName, message);
            Console.WriteLine($"Delivered '{deliveryReport.Value}' to '{deliveryReport.TopicPartitionOffset}'");
        }

        public void Dispose()
        {
            // Ensure all queued messages are delivered
            _producer.Flush();
            _producer.Dispose();
        }
    }
}
