using Confluent.Kafka;
using Newtonsoft.Json;

namespace Bbg.Producer.Web.Api.Services
{
    public class KafkaProducerService
    {
        private readonly IProducer<string, string> _producer;

        public KafkaProducerService(IConfiguration configuration)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"]
            };

            _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        }

        public async Task ProduceMessageAsync<T>(string topic, T events)
        {
            var jsonMessage = JsonConvert.SerializeObject(events);
            var message = new Message<string, string>
            {
                Key = "producer-net",
                Value = jsonMessage
            };

            var deliveryReport = await _producer.ProduceAsync(topic, message);
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
