// See https://aka.ms/new-console-template for more information
using Bogus;
using Confluent.Kafka;
using DummyData;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

// // int BlogId = 1;
// List<Blog> Blogs = new List<Blog>();
// // int PostId = 1;
// List<Post> Posts = new List<Post>();

// Faker f;
// Random random = new Random();

// DefaultContractResolver contractResolver = new DefaultContractResolver
// {
//     NamingStrategy = new CamelCaseNamingStrategy()
// };

// var config = new ProducerConfig
// {
//     BootstrapServers = "devops1:9092",
//     Acks = Acks.All,
//     AllowAutoCreateTopics = true,
// };

// using var p = new ProducerBuilder<int, string>(config).Build();

// try
// {
//     Init(100);
//     foreach (var blog in Blogs)
//     {
//         var dr = await p.ProduceAsync("blogs", new Message<int, string>
//         {
//             Key = blog.BlogId,
//             Value = JsonConvert.SerializeObject(blog, new JsonSerializerSettings
//             {
//                 ContractResolver = contractResolver,
//                 Formatting = Formatting.Indented
//             })
//         });
//         Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
//     }
//     foreach (var post in Posts)
//     {
//         var dr = await p.ProduceAsync("posts", new Message<int, string>
//         {
//             Key = post.PostId,
//             Value = JsonConvert.SerializeObject(post, new JsonSerializerSettings
//             {
//                 ContractResolver = contractResolver,
//                 Formatting = Formatting.Indented
//             })
//         });
//         Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
//     }
// }
// catch (ProduceException<Null, string> e)
// {
//     Console.WriteLine($"Delivery failed: {e.Error.Reason}");
// }

// void Init(int count)
// {
//     f = new Faker();

//     GenerateBlogs(count);

//     GeneratePost(count);
// }

// void GenerateBlogs(int blogCount)
// {
//     for (var i = 0; i < blogCount; i++)
//     {
//         var blog = new Blog
//         {
//             BlogId = random.Next(1, 5),
//             Url = f.Internet.Url()
//         };

//         Blogs.Add(blog);
//     };
// }

// void GeneratePost(int postCount)
// {
//     for (var i = 0; i < postCount; i++)
//     {
//         var post = new Post
//         {
//             PostId = random.Next(1, 5),
//             Title = f.Hacker.Phrase(),
//             Content = f.Lorem.Sentence(),
//             BlogId = random.Next(1, 5)
//         };

//         Posts.Add(post);
//     }
// }


// var results = Blogs.Join(Posts,
//   b => b.BlogId,
//   p => p.BlogId,
//   (blog, post) => new { blog.BlogId, post.PostId, blog.Url, post.Content, post.Title }
// );

// foreach (var blog in results)
// {
//     Console.WriteLine($"{blog.PostId} {blog.BlogId} - {blog.Url} - {blog.Title} - {blog.Content}");
// }
// Console.WriteLine(JsonConvert.SerializeObject(Blogs));
// Console.WriteLine(JsonConvert.SerializeObject(Posts));

Faker f = new Faker();
var persons = new List<Person>();
var personsInfo = new List<PersonInfo>();

DefaultContractResolver contractResolver = new DefaultContractResolver
{
    NamingStrategy = new CamelCaseNamingStrategy()
};

var names = new[] { "Shirley", "Zachary" };

async Task GeneratePerson(int count)
{
    await Task.Delay(250);
    for (var i = 0; i < count; i++)
    {

        var timestamp = DateTime.Now.AddSeconds(i);
        var person = new Person
        {
            Name = f.PickRandom(names),
            JobTitle = f.Name.JobTitle(),
            Proctime = timestamp.ToString("yyyy-MM-dd HH:mm:ss'Z'", new System.Globalization.CultureInfo("en-US"))
        };

        persons.Add(person);
    }
}

async Task GeneratePersonInfo(int count)
{
    await Task.Delay(250);

    for (var i = 0; i < count; i++)
    {
        var timestamp = DateTime.Now.AddSeconds(i);
        var personInfo = new PersonInfo
        {
            Name = f.PickRandom(names),
            AccountName = f.Finance.AccountName(),
            Amount = f.Finance.Amount(),
            TransactionType = f.Finance.TransactionType(),
            Proctime = timestamp.ToString("yyyy-MM-dd HH:mm:ss'Z'", new System.Globalization.CultureInfo("en-US"))
        };
        personsInfo.Add(personInfo);
    }
}

async Task Init(int count)
{
    var generatePerson = GeneratePerson(count);
    var generatePersonInfo = GeneratePersonInfo(count);
    await Task.WhenAll(generatePerson, generatePersonInfo);
}

// await Init(15);

var config = new ProducerConfig
{
    BootstrapServers = "devops1:9092",
    Acks = Acks.All,
    AllowAutoCreateTopics = true,
    BatchSize = 32 * 1024,
    LingerMs = 20
};

async Task ProduceMessageKafka<T>(List<T> list)
{
    await Task.Delay(250);
    foreach (var item in list)
    {
        using var producer = new ProducerBuilder<Null, string>(config).Build();
        if (item is Person person)
        {
            var dr = await producer.ProduceAsync("persons", new Message<Null, string>
            {
                Value = JsonConvert.SerializeObject(person, new JsonSerializerSettings
                {
                    ContractResolver = contractResolver,
                    Formatting = Formatting.Indented
                })
            });
            Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
        }
        else if (item is PersonInfo personInfo)
        {
            var dr = await producer.ProduceAsync("personsInfo", new Message<Null, string>
            {
                Value = JsonConvert.SerializeObject(personInfo, new JsonSerializerSettings
                {
                    ContractResolver = contractResolver,
                    Formatting = Formatting.Indented
                })
            });
            Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
        }
    }
}

try
{
    await Init(20);
    // await Task.WhenAll(ProduceMessageKafka(persons), ProduceMessageKafka(personsInfo));
    // await Task.WhenAll(ProduceMessageKafka(persons));
    await Task.WhenAll(ProduceMessageKafka(personsInfo));
}
catch (ProduceException<Null, string> e)
{
    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
}


public class Blog
{
    public int BlogId { get; set; }
    public string? Url { get; set; }
}


public class Post
{
    public int PostId { get; set; }
    public string? Title { get; set; }
    public string? Content { get; set; }
    public int BlogId { get; set; }
}

public class Person
{
    public string Name { get; set; } = default!;
    public string JobTitle { get; set; } = default!;
    public string Proctime { get; set; } = default!;
}

public class PersonInfo
{
    public string Name { get; set; } = default!;
    public string AccountName { get; set; } = default!;
    public decimal Amount { get; set; }
    public string TransactionType { get; set; } = default!;
    public string Proctime { get; set; } = default!;
}