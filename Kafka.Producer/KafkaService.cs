﻿using Confluent.Kafka.Admin;
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
        internal async Task CreateTopicAsync(string topicName)
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
                    new TopicSpecification(){ Name= topicName, NumPartitions = 3, ReplicationFactor = 1 }
                }); // 3 partitions and 1 replication factor for the topic

                Console.WriteLine($"Topic({topicName}) is created.");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        internal async Task SendSimpleMessageWithNullKey(string topicName)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9094"
            };

            using var producer = new ProducerBuilder<Null, string>(config).Build();

            foreach (var item in Enumerable.Range(1, 10))
            {
                var message = new Message<Null, string>()
                {
                    Value = $"Message(use case - 1){item}"
                };

                var result = await producer.ProduceAsync(topicName, message);

                foreach (var propertyInfo in result.GetType().GetProperties())
                {
                    Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
                }

                Console.WriteLine("---------------------------------");
                await Task.Delay(200);
            }
        }
    }
}
