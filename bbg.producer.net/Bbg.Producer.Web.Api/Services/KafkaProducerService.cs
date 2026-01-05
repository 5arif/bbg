using Bbg.Producer.Web.Api.Models;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace Bbg.Producer.Web.Api.Services
{
    public class KafkaProducerService
    {
        private readonly IProducer<string, string> _producer;
        private const string _topicName = "orders.events";

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
                Key = "producer-net", // Key can be a simple string
                Value = jsonMessage
            };            
            var deliveryReport = await _producer.ProduceAsync(_topicName, message);
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
