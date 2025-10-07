using System.Text.Json.Serialization;

namespace ABC_RETAIL_MVC.Models
{
    public class Customers
    {
        [JsonPropertyName("customerId")]
        public string? CustomerId { get; set; }

        [JsonPropertyName("customerName")]

        public string? CustomerName { get; set; }

        [JsonPropertyName("customeremail")]

        public string? CustomerEmail { get; set; }

        [JsonPropertyName("customerphone")]

        public string? CustomerPhone { get; set; }

        [JsonPropertyName("customeraddress")]
        public string? CustomerAddress { get; set; }

        [JsonPropertyName("timestamp")]

        public DateTimeOffset? Timestamp { get; set; }
    }
}
