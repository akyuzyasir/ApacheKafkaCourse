
using Kafka.Consumer;

Console.WriteLine("Kafka Consumer 1");
// We added try-catch block to catch the exception if the topicName is null, empty, or whitespace.
try
{
    var topicName = "use-case-2-topic";
    var kafkaService = new KafkaConsumerService();

    // we have 3 partitions and 1 replication factor for the topic. So, we can run 3 consumers at the same time and each consumer will consume messages from a different partition. But if we run more than 3 consumers, some of them will be idle because we have only 3 partitions.
    await kafkaService.ConsumeSimpleMessageWithIntKey(topicName);
}
catch (ArgumentException ex)
{
    Console.WriteLine($"Error: {ex.Message}");
}

Console.ReadLine(); // To keep the console open
