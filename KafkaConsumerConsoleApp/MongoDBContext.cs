using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaConsumerConsoleApp
{
    public class MongoDBContext
    {
        private readonly IMongoDatabase _database;

        public MongoDBContext(string connectionString, string databaseName)
        {
            var client = new MongoClient(connectionString);
            _database = client.GetDatabase(databaseName);
        }

        public IMongoCollection<OrderCreatedEvent> OrderCreatedEvents => _database.GetCollection<OrderCreatedEvent>("OrderCreatedEvents");

        public void InsertManyOrderCreatedEvents(List<OrderCreatedEvent> events)
        {
            OrderCreatedEvents.InsertMany(events);
        }
    }
}
