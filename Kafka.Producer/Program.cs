
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaProducerService();
var topicName = "use-case-2-topic";
await kafkaService.CreateTopicAsync(topicName);
await kafkaService.SendSimpleMessageWithIntKey(topicName);

