using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Shared.Events;
using Shared.Events.SerializersAndDesirializers;
using Stock.API.Services;
using System.Text.RegularExpressions;

namespace Producer.API.Services;
// This class is responsible for sending messages to the Kafka bus and creating topics or queues.
public class Bus(IConfiguration configuration) : IBus
{
    public ConsumerConfig GetConsumerConfig(string groupId)
    {
        return new ConsumerConfig()
        {
            BootstrapServers = configuration.GetSection("BusSettings").GetSection("Kafka")["BootstrapServers"],
            GroupId = groupId,
            Acks = Acks.All,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
    }
}
