using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace ABC_RETAIL_MVC.Models
{
    public class Products
    {
        [JsonPropertyName("productId")]
        public string? ProductId { get; set; }
        [JsonPropertyName("productName")]
        public string? ProductName { get; set; }

        [JsonPropertyName("productprice")]
        public double ProductPrice { get; set; }

        [JsonPropertyName("stock")]
        public int Stock { get; set; }

        [JsonPropertyName("productdescription")]
        public string? ProductDescription { get; set; }

        [Required]
        [Display(Name = "Product Image")]
        public IFormFile? ProductImage { get; set; }

        [JsonPropertyName("timestamp")]
        public DateTimeOffset? Timestamp { get; set; }
    }
}
