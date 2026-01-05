namespace Bbg.Producer.Web.Api.Models
{
    public class OrderEvent
    {
        public string orderId { get; set; }
        public double amount { get; set; }
        public string customer { get; set; }
    }
}
