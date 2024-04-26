using Bogus;
using Bogus.DataSets;
using Newtonsoft.Json;

namespace DummyData;

public class CurrencyDump
{
    Faker faker = new Faker();
    static List<CurrencyRate> currencyRates = new List<CurrencyRate>();
    static List<Order> orders = new List<Order>();
    int total = 0;

    // static string[] currencies = new string[] { "Yen", "Euro", "USD" };
    static string[] currencies = new string[] { "Euro", "Euro", "Euro" };
    async Task GenerateCurrencyRate(int count)
    {
        await Task.Delay(100);

        for (var i = 0; i < count; i++)
        {
            var timestamp = DateTime.Now.AddSeconds(i);
            var currencyRate = new CurrencyRate
            {
                UpdateTime = timestamp.ToString("HH:mm:ss", new System.Globalization.CultureInfo("en-US")),
                // Currency = faker.Finance.Currency().Code,
                Currency = faker.PickRandom(currencies),
                Rate = faker.Random.Int(1, 150),
                Proctime = timestamp.ToString("yyyy-MM-dd HH:mm:ss'Z'", new System.Globalization.CultureInfo("en-US"))
            };

            currencyRates.Add(currencyRate);
            await ProduceMsgToKafka.ProduceMessageKafka(currencyRates);
        }
    }

    async Task GenerateOrder(int count)
    {
        await Task.Delay(100);

        for (var i = 0; i < count; i++)
        {
            var timestamp = DateTime.Now.AddSeconds(5);
            var order = new Order
            {
                OrderTime = timestamp.ToString("HH:mm:ss", new System.Globalization.CultureInfo("en-US")),
                Amount = faker.Random.Int(1, 150),
                // Currency = faker.Finance.Currency().Code,
                Currency = faker.PickRandom(currencies),
                Proctime = timestamp.ToString("yyyy-MM-dd HH:mm:ss'Z'", new System.Globalization.CultureInfo("en-US"))
            };
            orders.Add(order);

            total += order.Amount;

            Console.WriteLine($"Total: {total}");

            await ProduceMsgToKafka.ProduceMessageKafka(orders);
        }
    }

    public async Task Init(int count)
    {
        // var generateCurrencyRate = GenerateCurrencyRate(count);
        var generateOrder = GenerateOrder(count);
        // await Task.WhenAll(generateCurrencyRate, generateOrder);
        await Task.WhenAll(generateOrder);


    }
}

public class CurrencyRate
{
    [JsonProperty("update_time")]
    public string? UpdateTime { get; set; }
    [JsonProperty("currency")]
    public string? Currency { get; set; }
    [JsonProperty("rate")]
    public int Rate { get; set; }
    [JsonProperty("proctime")]
    public string Proctime { get; set; } = default!;
}

public class Order
{
    [JsonProperty("order_time")]
    public string? OrderTime { get; set; }
    [JsonProperty("amount")]
    public int Amount { get; set; }
    [JsonProperty("currency")]
    public string? Currency { get; set; }
    [JsonProperty("proctime")]
    public string Proctime { get; set; } = default!;
}