using System;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Queues.Models;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace Queue_Function;

//Name: CLDV6212 Azure functions part 1 Getting the basics out the way HTTP Trigger
//Author: IIE Emeris School of Computer Science
//Url: https://youtu.be/l7s5u-QzYe8?si=UKfsznYR12jxdEa9
//Date accessed: 05 October 2025

//Name: CLDV6212 Azure functions part 2 Azure functions and queues triggers
//Author:IIE Emeris School of Computer Science
//Url: https://youtu.be/zP4umzRCsTM?si=-gG6a3p06R7kQtHy
//Date accessed: 05 October 2025

//Name: CLDV6212 Azure functions part 3 Azure functions and MVC
//Author: IIE Emeris School of Computer Science
//Url: https://youtu.be/x7yTh85fQbw?si=YVjxUyzChioyR2jb
//Date accessed: 05 October 2025

//Name: CLDV6212 Azure functions part 4 Azure functions and MVC and blobs
//Author:IIE Emeris School of Computer Science
//Url: https://youtu.be/r-VksPFfFpE?si=YBzXZTbKv4wVDbT8
//Date accessed: 05 October 2025

//Name: CLDV6212 Azure functions part 5 Azure functions publish
//Author: IIE Emeris School of Computer Science
//Url: https://youtu.be/GXGN-aWbwO0?si=OQcVvKZUEsLoIDtL
//Date accessed: 05 October 2025

public class Function1
{
    private readonly ILogger<Function1> _logger;
    private readonly string? _storageConnectionString;
    private readonly TableClient _ordersTable;
    private readonly TableClient _productsTable;
    private readonly TableClient _customersTable;
    private BlobContainerClient _blobContainerClient;

    public Function1(ILogger<Function1> logger)
    {
        _logger = logger;
        _storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=nmathenjwa;AccountKey=IhEaHkKo6KTuwo6VB6NnTzCcyjHy6Lkb7n8xw/gekIUeX3p3X5AdU8sL5KCXaQ3bUCN9JhKLTnF++AStIZaNEw==;EndpointSuffix=core.windows.net";

        var serviceClient = new TableServiceClient(_storageConnectionString);
        _ordersTable = serviceClient.GetTableClient("Orders");
        _productsTable = serviceClient.GetTableClient("Products");
        _customersTable = serviceClient.GetTableClient("Customers");

        _blobContainerClient = new BlobContainerClient(
            _storageConnectionString, "product-images"
            );
        _blobContainerClient.CreateIfNotExists(
            Azure.Storage.Blobs.Models.PublicAccessType.Blob
            );
    }

    [Function(nameof(OrdersQueue))]
    public async Task OrdersQueue([QueueTrigger("processed-orders", Connection = "connection")] QueueMessage message)
    {
        _logger.LogInformation($"C# Queue trigger function processed: {message.MessageText}");

        //create table if not exists
        await _ordersTable.CreateIfNotExistsAsync();

        //1. manually deserialize the message
        var order = JsonSerializer.Deserialize<Order>(message.MessageText);

        if (order == null)
        {
            _logger.LogError("Failed to deserialize message.");
            return;
        }

        //2. set the required properties
        order.RowKey = Guid.NewGuid().ToString();
        order.PartitionKey = "Orders";

        _logger.LogInformation($"Saving entity with RowKey: {order.RowKey}");

        //3. manually add entity to table
        await _ordersTable.AddEntityAsync(order);
        _logger.LogInformation("Entity saved successfully to the table");
    }

    [Function(nameof(ProductsQueue))]
    public async Task ProductsQueue(
    [QueueTrigger("product-queue", Connection = "connection")] QueueMessage message)
    {
        _logger.LogInformation("Triggered ProductsQueue function for product message.");

        // Create the table if not exists
        await _productsTable.CreateIfNotExistsAsync();

        // 1️⃣ Deserialize the message
        Products? product;
        try
        {
            product = JsonSerializer.Deserialize<Products>(message.MessageText);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "❌ Failed to deserialize product message.");
            return;
        }

        if (product == null)
        {
            _logger.LogError("❌ Product message was null after deserialization.");
            return;
        }

        // 2️⃣ Set partition and row keys
        product.PartitionKey = "Products";
        product.RowKey = Guid.NewGuid().ToString();

        _logger.LogInformation($"🆕 Processing Product: {product.ProductName} (RowKey: {product.RowKey})");

        // 3️⃣ Upload image to Blob Storage (if image data exists)
        if (!string.IsNullOrEmpty(product.ProductImageUrl))
        {
            try
            {
                _logger.LogInformation("📦 Image data detected, preparing to upload to Blob Storage...");

                // Convert Base64 to bytes
                byte[] imageBytes = Convert.FromBase64String(product.ProductImageUrl);

                // Create unique blob name
                string blobName = $"{product.RowKey}.jpg";

                // Create blob client
                var blobClient = _blobContainerClient.GetBlobClient(blobName);

                // Upload to Azure Blob Storage
                using var stream = new MemoryStream(imageBytes);
                await blobClient.UploadAsync(stream, overwrite: true);

                // Store blob URL in product record
                product.ProductImageUrl = blobClient.Uri.ToString();

                _logger.LogInformation($"✅ Image uploaded successfully to Blob Storage: {product.ProductImageUrl}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ Failed to upload image to Blob Storage.");
            }
        }
        else
        {
            _logger.LogInformation("ℹ️ No image data found for this product. Skipping blob upload.");
        }

        // 4️⃣ Save product to Table Storage
        try
        {
            await _productsTable.AddEntityAsync(product);
            _logger.LogInformation("✅ Product entity saved successfully to Azure Table Storage.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "❌ Failed to save product entity to Azure Table Storage.");
        }
    }



    [Function(nameof(CustomersQueue))]
    public async Task CustomersQueue([QueueTrigger("customer-queue", Connection = "connection")] QueueMessage message)
    {
        _logger.LogInformation($"C# Queue trigger function processed: {message.MessageText}");

        //create table if not exists
        await _customersTable.CreateIfNotExistsAsync();

        //1. manually deserialize the message
        var customer = JsonSerializer.Deserialize<Customers>(message.MessageText);

        if (customer == null)
        {
            _logger.LogError("Failed to deserialize message.");
            return;
        }

        //2. set the required properties
        customer.RowKey = Guid.NewGuid().ToString();
        customer.PartitionKey = "Customers";

        _logger.LogInformation($"Saving entity with RowKey: {customer.RowKey}");

        //3. manually add entity to table
        await _customersTable.AddEntityAsync(customer);
        _logger.LogInformation("Entity saved successfully to the table");
    }

    [Function(nameof(GetOrders))]
    public async Task<HttpResponseData> GetOrders(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "Orders")] HttpRequestData req
        )
    {
        _logger.LogInformation("C# HTTP trigger function processed a request to get orders.");

        try
        {
            var orders = await _ordersTable.QueryAsync<Order>().ToListAsync();

            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(orders);
            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve orders.");

            var response = req.CreateResponse(HttpStatusCode.InternalServerError);
            await response.WriteStringAsync("Failed to retrieve orders.");
            return response;
        }
    }

    [Function(nameof(GetProducts))]
    public async Task<HttpResponseData> GetProducts(
    [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "Products")] HttpRequestData req
    )
    {
        _logger.LogInformation("C# HTTP trigger function processed a request to get products.");

        try
        {
            var products = await _productsTable.QueryAsync<Products>().ToListAsync();

            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(products);
            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve products.");

            var response = req.CreateResponse(HttpStatusCode.InternalServerError);
            await response.WriteStringAsync("Failed to retrieve products.");
            return response;
        }
    }

    [Function(nameof(GetCustomers))]
    public async Task<HttpResponseData> GetCustomers(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "Customers")] HttpRequestData req
        )
    {
        _logger.LogInformation("C# HTTP trigger function processed a request to get customers.");

        try
        {
            var customers = await _customersTable.QueryAsync<Customers>().ToListAsync();


            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(customers);
            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve customers.");

            var response = req.CreateResponse(HttpStatusCode.InternalServerError);
            await response.WriteStringAsync("Failed to retrieve customers.");
            return response;
        }
    }

    private async Task<string> UploadFileToBlobAsync(string fileName, byte[] fileBytes)
    {
        var blobClient = _blobContainerClient.GetBlobClient(fileName);

        using var stream = new MemoryStream(fileBytes);
        await blobClient.UploadAsync(stream, overwrite: true);

        _logger.LogInformation($"File {fileName} uploaded to blob storage.");
        return blobClient.Uri.ToString(); // return the blob URL
    }



}

