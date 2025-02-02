using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Shared.Events;

namespace Order.API.Services;
// This class is responsible for sending messages to the Kafka bus and creating topics or queues.
public class Bus(IConfiguration configuration, ILogger<Bus> logger) : IBus
{
    private readonly ProducerConfig _config = new()
    {
        // We get the Kafka server address from the appsettings.json file. So we don't need to hardcode it.
        BootstrapServers = configuration.GetSection("BusSettings").GetSection("Kafka")["BootstrapServers"],
        Acks = Acks.All,
        MessageTimeoutMs = 6000,
        AllowAutoCreateTopics = true // default is true
    };
    // T1, T2: Key and Value types of the message to be sent. We made it generic in order to be able to send any type of message.
    public async Task<bool> Publish<T1, T2>(T1 key, T2 value, string topicOrQueueName)
    {
        // We create a producer object. We serialize the key and value objects with the CustomKeySerializer and CustomValueSerializer classes we created.
        using var producer = new ProducerBuilder<T1, T2>(_config).SetKeySerializer(new CustomKeySerializer<T1>()).SetValueSerializer(new CustomValueSerializer<T2>()).Build();
        // We create a message object 
        var message = new Message<T1, T2>()
        {
            Key = key,
            Value = value
        };
        //
        var result = await producer.ProduceAsync(topicOrQueueName, message);

        return result.Status == PersistenceStatus.Persisted; // If the message is sent successfully, the status will be "Persisted".
    }
    // We didn't name this method CreateTopic because later we may also integrate with RabbitMQ or other message brokers that use the term "queue" instead of "topic".
    public async Task CreateTopicsOrQueuesAsync(List<string> topicOrQueueNameList)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig()
        {
            BootstrapServers = configuration.GetSection("BusSettings").GetSection("Kafka")["BootstrapServers"]
        }).Build();

        try
        {
            foreach (var topicOrQueueName in topicOrQueueNameList)
            {
                await adminClient.CreateTopicsAsync(
                [
                    new TopicSpecification()
                    {
                        Name = topicOrQueueName,
                        NumPartitions = 6,
                        ReplicationFactor = 1
                    }
                ]);
                // We log the information that the topic or queue is created.
                logger.LogInformation($"Topic({topicOrQueueName}) is created.");
            }

        }
        catch (Exception e)
        {
            logger.LogWarning(e.Message);
        }
    }
}
