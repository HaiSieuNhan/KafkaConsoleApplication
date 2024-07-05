using System;
using Confluent.Kafka;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using KafkaConsumerConsoleApp;

public class OrderCreatedEvent
{
    public Guid OrderId { get; set; }
    public string ProductName { get; set; }
    public int Quantity { get; set; }
    public decimal Price { get; set; }
	public DateTime CreatedDate { get; set; }
}

public class EventConsumer
{
    private readonly IConsumer<Null, string> _consumer;
    private readonly MongoDBContext _dbContext;
    private readonly List<OrderCreatedEvent> _batchEvents;
    private readonly int _batchSize = 3000; // Kích thước lô
    private readonly SemaphoreSlim _batchLock;

    public EventConsumer(string brokerList, string groupId, string mongoConnectionString, string mongoDatabaseName)
    {
        var config = new ConsumerConfig
        {
            GroupId = groupId,
            BootstrapServers = brokerList,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        _consumer = new ConsumerBuilder<Null, string>(config).Build();
        _consumer.Subscribe("order_events");

        _dbContext = new MongoDBContext(mongoConnectionString, mongoDatabaseName);
        _batchEvents = new List<OrderCreatedEvent>();
        _batchLock = new SemaphoreSlim(1, 1); // SemaphoreSlim để đồng bộ hóa việc thêm sự kiện vào danh sách lô
    }

    public void ConsumeOrderCreatedEvents(CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                var cr = _consumer.Consume(cancellationToken);
                var orderEvent = JsonConvert.DeserializeObject<OrderCreatedEvent>(cr.Message.Value);
                Console.WriteLine($"OrderCreatedEvent consumed: {cr.Message.Value}");

				orderEvent.CreatedDate = DateTime.Now;

				// Thêm sự kiện vào danh sách lô
				_batchLock.Wait();
                _batchEvents.Add(orderEvent);
                _batchLock.Release();

                // Kiểm tra nếu danh sách lô đủ kích thước
                if (_batchEvents.Count >= _batchSize)
                {
                    SaveBatchEvents();
                }
            }
        }
        catch (OperationCanceledException)
        {
            _consumer.Close();
            // Lưu các sự kiện còn lại trong danh sách lô trước khi thoát
            if (_batchEvents.Count > 0)
            {
                SaveBatchEvents();
            }
        }
    }

    private void SaveBatchEvents()
    {
        // Đảm bảo không có ai khác đang thêm sự kiện vào danh sách lô
        _batchLock.Wait();
        try
        {
            // Sao chép danh sách sự kiện để ghi theo lô và đặt danh sách lô mới
            var eventsToSave = new List<OrderCreatedEvent>(_batchEvents);
            _batchEvents.Clear();

            // Ghi danh sách sự kiện vào MongoDB
            _dbContext.InsertManyOrderCreatedEvents(eventsToSave);
            Console.WriteLine($"Batch of {eventsToSave.Count} OrderCreatedEvents saved to MongoDB.");
        }
        finally
        {
            _batchLock.Release();
        }
    }
}

class Program
{
    static void Main(string[] args)
    {
        var cancellationTokenSource = new CancellationTokenSource();

        var consumer = new EventConsumer(
            "localhost:9092",
            "order_event_group",
			"mongodb://root:example@localhost:27017/",
            "OrderManagement"
        );

        Task.Run(() => consumer.ConsumeOrderCreatedEvents(cancellationTokenSource.Token));

        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
        cancellationTokenSource.Cancel();
    }
}
