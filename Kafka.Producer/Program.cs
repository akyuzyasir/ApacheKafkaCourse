
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaService();
var topicName = "use-case-1-topic";
await kafkaService.CreateTopicAsync(topicName);
await kafkaService.SendSimpleMessageWithNullKey(topicName);

