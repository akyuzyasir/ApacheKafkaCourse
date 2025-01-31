
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaProducerService();
var topicName = "retry-topic";
await kafkaService.CreateTopicRetryWithClusterAsync(topicName);
await kafkaService.SendMessageWithRetryToCluster(topicName);

Console.WriteLine("Messages are sent to the Kafka server.");

Console.ReadLine();
