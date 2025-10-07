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
        _storageConnectionString = Environment.GetEnvironmentVariable("connection");

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
    public async Task ProductsQueue([QueueTrigger("product-queue", Connection = "connection")] QueueMessage message)
    {
        _logger.LogInformation($"C# Queue trigger function processed: {message.MessageText}");

        //create table if not exists
        await _productsTable.CreateIfNotExistsAsync();

        //1. manually deserialize the message
        var product = JsonSerializer.Deserialize<Products>(message.MessageText);

        if (product == null)
        {
            _logger.LogError("Failed to deserialize message.");
            return;
        }

        //2. set the required properties
        product.RowKey = Guid.NewGuid().ToString();
        product.PartitionKey = "Products";

        _logger.LogInformation($"Saving entity with RowKey: {product.RowKey}");

        //3. manually add entity to table
        await _productsTable.AddEntityAsync(product);
        _logger.LogInformation("Entity saved successfully to the table");
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


}

