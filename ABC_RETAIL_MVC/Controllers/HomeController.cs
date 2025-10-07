using System.Diagnostics;
using System.Text.Json;
using ABC_RETAIL_MVC.Models;
using Microsoft.AspNetCore.Mvc;

namespace ABC_RETAIL_MVC.Controllers
{
    public class HomeController : Controller
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IConfiguration _configuration;

        public HomeController(IHttpClientFactory httpClientFactory, IConfiguration configuration)
        {
           _httpClientFactory = httpClientFactory;
              _configuration = configuration;
        }

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Privacy()
        {
            return View();
        }

        public async Task<IActionResult> Products()
        {
            var httpClient = _httpClientFactory.CreateClient();
            var apiBaseUrl = _configuration["FunctionApi:BaseUrl"];
            try
            {
                var httpResponseMessage = await
                    httpClient.GetAsync($"{apiBaseUrl}products");
                if (httpResponseMessage.IsSuccessStatusCode)
                {
                    var contentStream = await httpResponseMessage.Content.ReadAsStreamAsync();
                    var options = new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    };
                    var products = await JsonSerializer.DeserializeAsync<IEnumerable<Products>>(
                        contentStream, options);
                    return View(products);
                }
            }
            catch (HttpRequestException)
            {
                ViewBag.ErrorMessage = "Could not connect to the API" + "Plese ensure the Azure Function is running.";
                return View(new List<Products>());
            }
            ViewBag.ErrorMessage = "Could not retrieve products from the API.";
            return View(new List<Products>());
        }

        public async Task<IActionResult> Customers()
        {
            var httpClient = _httpClientFactory.CreateClient();
            var apiBaseUrl = _configuration["FunctionApi:BaseUrl"];
            try
            {
                var httpResponseMessage = await
                    httpClient.GetAsync($"{apiBaseUrl}customers");
                if (httpResponseMessage.IsSuccessStatusCode)
                {
                    var contentStream = await httpResponseMessage.Content.ReadAsStreamAsync();
                    var options = new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    };
                    var customers = await JsonSerializer.DeserializeAsync<IEnumerable<Customers>>(
                        contentStream, options);
                    return View(customers);
                }
            }
            catch (HttpRequestException)
            {
                ViewBag.ErrorMessage = "Could not connect to the API" + "Plese ensure the Azure Function is running.";
                return View(new List<Customers>());
            }
            ViewBag.ErrorMessage = "Could not retrieve products from the API.";
            return View(new List<Customers>());
        }

        public async Task<IActionResult> Orders()
        {
            var httpClient = _httpClientFactory.CreateClient();
            var apiBaseUrl = _configuration["FunctionApi:BaseUrl"];

            try
            {
                var httpResponseMessage = await
                    httpClient.GetAsync($"{apiBaseUrl}orders");
                if (httpResponseMessage.IsSuccessStatusCode)
                {
                    var contentStream = await httpResponseMessage.Content.ReadAsStreamAsync();
                    var options = new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    };
                    var orders = await JsonSerializer.DeserializeAsync<IEnumerable<Orders>>(
                        contentStream, options);
                    return View(orders);
                }
            }
            catch (HttpRequestException)
            {
                ViewBag.ErrorMessage = "Could not connect to the API" + "Plese ensure the Azure Function is running.";
                return View(new List<Orders>());
            }
            ViewBag.ErrorMessage = "Could not retrieve products from the API.";
            return View(new List<Orders>());
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}
