using Confluent.Kafka;

namespace bbg_net_web_api.Services
{
    public class KafkaProducerService
    {
        private readonly IProducer<Null, string> _producer;
        private const string TopicName = "test";

        public KafkaProducerService(IConfiguration configuration)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"]
            };
            _producer = new ProducerBuilder<Null, string>(producerConfig).Build();
        }

        public async Task ProduceMessageAsync(string message)
        {
            var dr = await _producer.ProduceAsync(TopicName, new Message<Null, string> { Value = message });
            Console.WriteLine($"Delivered message to {dr.TopicPartitionOffset}");
        }
    }
}
