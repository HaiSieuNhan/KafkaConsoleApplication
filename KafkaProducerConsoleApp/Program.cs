using System;
using Confluent.Kafka;
using System.Threading.Tasks;
using Newtonsoft.Json;

public class OrderCreatedEvent
{
    public Guid OrderId { get; set; }
    public string ProductName { get; set; }
    public int Quantity { get; set; }
    public decimal Price { get; set; }
}

public class EventProducer
{
    private readonly IProducer<Null, string> _producer;

    public EventProducer(string brokerList)
    {
        var config = new ProducerConfig { BootstrapServers = brokerList };
        _producer = new ProducerBuilder<Null, string>(config).Build();
    }

    public async Task ProduceOrderCreatedEventAsync(List<OrderCreatedEvent> events)
    {
        var tasks = new List<Task>();
        foreach (var orderEvent in events)
        {
            var eventMessage = JsonConvert.SerializeObject(orderEvent);
            tasks.Add(_producer.ProduceAsync("order_events", new Message<Null, string> { Value = eventMessage }));
        }
        await Task.WhenAll(tasks);
        Console.WriteLine($"Sent {events.Count} events to Kafka.");
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        var producer = new EventProducer("localhost:9092");

        var events = new List<OrderCreatedEvent>();
        for (int i = 0; i < 1000000; i++)
        {
            var orderEvent = new OrderCreatedEvent
            {
                OrderId = Guid.NewGuid(),
                ProductName = $"Product{i}",
                Quantity = 1,
                Price = 100.00m + i
            };
            events.Add(orderEvent);
        }

        await producer.ProduceOrderCreatedEventAsync(events);
    }
}
