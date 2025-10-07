using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;

namespace Queue_Function
{
    public class Products: ITableEntity
    {
        public string? ProductId { get; set; }

        public string? ProductName { get; set; }

        public string? ProductDescription { get; set; }

        public double ProductPrice { get; set; }

        public string? ProductImageUrl { get; set; }

        // ITableEntity implementation

        public string PartitionKey { get; set; } = "Products";

        public string RowKey { get; set; } = string.Empty;

        public DateTimeOffset? Timestamp { get; set; }

        public ETag ETag { get; set; }

    }
}
