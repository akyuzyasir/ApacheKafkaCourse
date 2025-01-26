
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaProducerService();
var topicName = "use-case-3-topic";
await kafkaService.CreateTopicAsync(topicName);
await kafkaService.SendComplexMessageWithIntKeyAndHeader(topicName);

Console.WriteLine("Messages are sent to the Kafka server.");

