using Confluent.Kafka.Admin;
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Producer
{
    internal class KafkaService
    {
        private const string TopicName = "my-topic-one";

        internal async Task CreateTopicAsync()
        {
            // we use using to dispose the resources after the task is done
            using var adminClient = new AdminClientBuilder(new AdminClientConfig()
            {
                BootstrapServers = "localhost:9094" // This code is for local Kafka. 
            }).Build();

            try
            {
                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification(){ Name= TopicName, NumPartitions = 3, ReplicationFactor = 1 }
                }); // 3 partitions and 1 replication factor for the topic

                Console.WriteLine($"Topic({TopicName}) has been created.");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }
}
