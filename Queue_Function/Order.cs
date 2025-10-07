using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;

namespace Queue_Function
{
    public class Order: ITableEntity
    {
        public string? OrderId { get; set; }
        public string? CustomerName { get; set; }

        public string? ProductName { get; set; }

        public int Quantity { get; set; }

        public double? TotalAmount { get; set; }


        public string OrderStatus { get; set; } = "Pending";


        // ITableEntity properties

        public string PartitionKey { get; set; } = "Orders";

        public string RowKey { get; set; } = string.Empty;

        public DateTimeOffset? Timestamp { get; set; }

        public ETag ETag { get; set; }
    }
}
