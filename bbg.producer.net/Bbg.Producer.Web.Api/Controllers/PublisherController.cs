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

        public PublisherController(KafkaProducerService kafkaProducerService)
        {
            _kafkaProducerService = kafkaProducerService;
        }

        [HttpPost("publish")]
        public async Task<IActionResult> PublishMessage([FromBody] OrderEvent message)
        {
            await _kafkaProducerService.ProduceMessageAsync(message);
            return Ok(message);
        }
    }
}
