
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaProducerService();
var topicName = "use-case-1.1-topic";
await kafkaService.CreateTopicAsync(topicName);
await kafkaService.SendSimpleMessageWithNullKey(topicName);

