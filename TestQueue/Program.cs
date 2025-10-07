using System.Text.Json;
using Azure.Storage.Queues;

namespace TestQueue
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var connectionString = "DefaultEndpointsProtocol=https;AccountName=nmathenjwa;AccountKey=IhEaHkKo6KTuwo6VB6NnTzCcyjHy6Lkb7n8xw/gekIUeX3p3X5AdU8sL5KCXaQ3bUCN9JhKLTnF++AStIZaNEw==;EndpointSuffix=core.windows.net";

            // Send order
            var queueClient = new QueueClient(
                connectionString,
                "proccessed-orders",
                new QueueClientOptions { MessageEncoding = QueueMessageEncoding.Base64 });
                

            await queueClient.CreateIfNotExistsAsync();

            var order = new { OrderId = "O001", CustomerId = "C001", ProductId = "P001", Quantity = 2, OrderDate = DateTime.UtcNow };

            string json = JsonSerializer.Serialize(order);

            await queueClient.SendMessageAsync(json);

            Console.WriteLine($"Order message sent: {json}");

            // Send customer
            var customerQueueClient = new QueueClient(
                connectionString,
                "customers-queue",
                new QueueClientOptions { MessageEncoding = QueueMessageEncoding.Base64 });


            await queueClient.CreateIfNotExistsAsync();

            var customer = new { CustomerId = "C001", CustomerName = "Lihle", CustomerEmail = "lihle2@gmail.com", CustomerPhone = "0826364567", CustomerAddress = "156 Bendon Rd, Durban" };

            string customerJson = JsonSerializer.Serialize(customer);

            await customerQueueClient.SendMessageAsync(json);

            Console.WriteLine($"Customer message sent: {json}");

            // Send product
            var productQueueClient = new QueueClient(
                connectionString,
                "products-queue",
                new QueueClientOptions { MessageEncoding = QueueMessageEncoding.Base64 });

            await queueClient.CreateIfNotExistsAsync();

            var product = new { ProductId = "P001", ProductName = "Laptop", ProductDescription = "A high-performance laptop", Price = 1200.00, StockQuantity = 10 };

            string productJson = JsonSerializer.Serialize(product);

            await productQueueClient.SendMessageAsync(json);

            Console.WriteLine($"Product message sent: {json}");
        }
    }
}
