using System.Text.Json.Serialization;

namespace ABC_RETAIL_MVC.Models
{
    public class Orders
    {
        [JsonPropertyName("orderId")]
        public string? OrderId { get; set; }

        [JsonPropertyName("customerId")]
        public int CustomerId { get; set; }

        [JsonPropertyName("productId")]
        public int ProductId { get; set; }

        [JsonPropertyName("orderDate")]

        public DateTime OrderDate { get; set; }

        [JsonPropertyName("orderStatus")]
        public string? OrderStatus { get; set; }


        [JsonPropertyName("totalAmount")]
        public double? TotalAmount { get; set; }

        [JsonPropertyName("timestamp")]
        public DateTimeOffset? Timestamp { get; set; }

        [JsonPropertyName("quantity")]
        public int Quantity { get; set; }
    }
}
