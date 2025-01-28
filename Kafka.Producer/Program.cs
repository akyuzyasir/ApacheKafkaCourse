
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer;

Console.WriteLine("Kafka Producer");


var kafkaService = new KafkaProducerService();
var topicName = "mycluster-topic-2";
await kafkaService.CreateTopicWithClusterAsync(topicName);
await kafkaService.SendMessageToCluster(topicName);

Console.WriteLine("Messages are sent to the Kafka server.");

