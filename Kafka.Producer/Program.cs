
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaProducerService();
var topicName = "ack-topic";
await kafkaService.CreateTopicAsync(topicName);
await kafkaService.SendMessageWithAck(topicName);

Console.WriteLine("Messages are sent to the Kafka server.");

