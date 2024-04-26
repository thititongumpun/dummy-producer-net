using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace DummyData;

public class ProduceMsgToKafka
{
    public static async Task ProduceMessageKafka<T>(List<T> list)
    {
        var config = new ProducerConfig
        {
            // BootstrapServers = "devops1:9092",
            BootstrapServers = "streaming-dev.xyz:29092",
            Acks = Acks.All,
            AllowAutoCreateTopics = true,
            BatchSize = 32 * 1024,
            LingerMs = 20
        };

        DefaultContractResolver contractResolver = new DefaultContractResolver
        {
            NamingStrategy = new CamelCaseNamingStrategy()
        };

        foreach (var item in list)
        {
            using var producer = new ProducerBuilder<Null, string>(config).Build();
            if (item is Order order)
            {
                var dr = await producer.ProduceAsync("orders", new Message<Null, string>
                {
                    Value = JsonConvert.SerializeObject(order, new JsonSerializerSettings
                    {
                        ContractResolver = contractResolver,
                        Formatting = Formatting.Indented
                    })
                });
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
            else if (item is CurrencyRate currencyRate)
            {
                var dr = await producer.ProduceAsync("currencyRates", new Message<Null, string>
                {
                    Value = JsonConvert.SerializeObject(currencyRate, new JsonSerializerSettings
                    {
                        ContractResolver = contractResolver,
                        Formatting = Formatting.Indented
                    })
                });
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
        }
    }
}