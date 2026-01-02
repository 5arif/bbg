using bbg_net_web_api.Services;
using Microsoft.AspNetCore.Mvc;

namespace bbg_net_web_api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PublisherController : ControllerBase
    {
        private readonly KafkaProducerService _kafkaProducerService;

        public PublisherController(KafkaProducerService kafkaProducerService)
        {
            _kafkaProducerService = kafkaProducerService;
        }

        [HttpPost("publish")]
        public async Task<IActionResult> PublishMessage([FromBody] string message)
        {
            await _kafkaProducerService.ProduceMessageAsync(message);
            return Ok(message);
        }
    }
}
