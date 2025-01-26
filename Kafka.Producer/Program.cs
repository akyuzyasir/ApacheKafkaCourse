
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaProducerService();
var topicName = "use-case-4-topic";
await kafkaService.CreateTopicAsync(topicName);
await kafkaService.SendMessageToSpecifiedPartition(topicName);

Console.WriteLine("Messages are sent to the Kafka server.");

