using Confluent.Kafka;

namespace bbg_net_web_api.Services
{
    public class KafkaProducerService
    {
        private readonly IProducer<string, string> _producer;
        private const string TopicName = "test";

        public KafkaProducerService(IConfiguration configuration)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"]
            };
            _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        }

        public async Task ProduceMessageAsync(string key, string message)
        {
            var dr = await _producer.ProduceAsync(TopicName, new Message<string, string> { Key = key, Value = message });
            Console.WriteLine($"Delivered message to {dr.TopicPartitionOffset}");
        }
    }
}
