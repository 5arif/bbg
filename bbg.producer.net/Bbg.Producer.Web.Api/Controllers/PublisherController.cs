using Bbg.Producer.Web.Api.Models;
using Bbg.Producer.Web.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace Bbg.Producer.Web.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PublisherController : ControllerBase
    {
        private readonly KafkaProducerService _kafkaProducerService;

        private const string _orderTopicName = "orders.events";
        private const string _serviceTopicName = "services.events";

        public PublisherController(KafkaProducerService kafkaProducerService)
        {
            _kafkaProducerService = kafkaProducerService;
        }

        [HttpPost("publish")]
        public async Task<IActionResult> PublishMessage([FromBody] OrderEvent message)
        {
            await _kafkaProducerService.ProduceMessageAsync(_orderTopicName, message);
            return Ok(message);
        }

        [HttpPost("publish-service")]
        public async Task<IActionResult> PublishService([FromBody] ServiceEvent message)
        {
            await _kafkaProducerService.ProduceMessageAsync(_serviceTopicName, message);
            return Ok(message);
        }
    }
}
