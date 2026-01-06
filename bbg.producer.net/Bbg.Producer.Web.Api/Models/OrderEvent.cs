namespace Bbg.Producer.Web.Api.Models
{
    public class OrderEvent
    {
        public string OrderId { get; set; }
        public double Amount { get; set; }
        public string Customer { get; set; }
    }
}
