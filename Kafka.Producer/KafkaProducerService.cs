﻿using Confluent.Kafka.Admin;
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kafka.Producer.Events;
using Kafka.Product.Events;

namespace Kafka.Producer;

internal class KafkaProducerService
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
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));

            var topicExists = metadata.Topics.Any(m => m.Topic == topicName);

            // We can set the configuration for the topic. In this example, we set the message.timestamp.type to LogAppendTime. LogAppendTime is the time when the message is appended to the log.
            if (!topicExists)
            {
                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification(){ Name= topicName, NumPartitions = 6, ReplicationFactor = 1}
                }); // 3 partitions and 1 replication factor for the topic
                Console.WriteLine($"Topic({topicName}) is created.");
            }
            else
            {
                Console.WriteLine($"Topic({topicName}) already exists.");
            }

        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }
    internal async Task CreateTopicWithRetentionAsync(string topicName)
    {
        // we use using to dispose the resources after the task is done
        using var adminClient = new AdminClientBuilder(new AdminClientConfig()
        {
            BootstrapServers = "localhost:9094" // This code is for local Kafka. 
        }).Build();

        try
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));

            var topicExists = metadata.Topics.Any(m => m.Topic == topicName);

            // We can set the configuration for the topic. In this example, we set the message.timestamp.type to LogAppendTime. LogAppendTime is the time when the message is appended to the log.
            if (!topicExists)
            {
                TimeSpan retentionSpan = TimeSpan.FromDays(30);

                var configs = new Dictionary<string, string>()
                {
                    //{"retention.byte", "1024" } // each partition will keep 1KB of messages.
                    //{"retention.ms","-1" } // -1 means that the messages will be kept forever.
                    {"retention.ms", retentionSpan.TotalMilliseconds.ToString()} // 30 days
                };

                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification()
                    {
                        Name= topicName,
                        NumPartitions = 6,
                        ReplicationFactor = 1,
                        Configs= configs
                    }
                }); // 6 partitions and 1 replication factor for the topic
                Console.WriteLine($"Topic({topicName}) is created.");
            }
            else
            {
                Console.WriteLine($"Topic({topicName}) already exists.");
            }

        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }
    internal async Task CreateTopicWithClusterAsync(string topicName)
    {
        // we use using to dispose the resources after the task is done
        using var adminClient = new AdminClientBuilder(new AdminClientConfig()
        {
            BootstrapServers = "localhost:7000,localhost:7001,localhost:7002" // This code is for local Kafka. 
        }).Build();

        try
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));

            var topicExists = metadata.Topics.Any(m => m.Topic == topicName);

            // We can set the configuration for the topic. In this example, we set the message.timestamp.type to LogAppendTime. LogAppendTime is the time when the message is appended to the log.
            if (!topicExists)
            {
                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification(){ Name= topicName, NumPartitions = 6, ReplicationFactor = 3}
                }); // 3 partitions and 1 replication factor for the topic
                Console.WriteLine($"Topic({topicName}) is created.");
            }
            else
            {
                Console.WriteLine($"Topic({topicName}) already exists.");
            }

        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }
    internal async Task CreateTopicRetryWithClusterAsync(string topicName)
    {
        // we use using to dispose the resources after the task is done
        using var adminClient = new AdminClientBuilder(new AdminClientConfig()
        {
            BootstrapServers = "localhost:7000,localhost:7001,localhost:7002" // This code is for local Kafka. 
        }).Build();

        try
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));

            var topicExists = metadata.Topics.Any(m => m.Topic == topicName);

            // We can set the configuration for the topic. In this example, we set the message.timestamp.type to LogAppendTime. LogAppendTime is the time when the message is appended to the log.
            if (!topicExists)
            {
                var configs = new Dictionary<string, string>()
                {
                    {"min.insync.replicas","3"}
                };
                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification(){ Name= topicName, NumPartitions = 6, ReplicationFactor = 3, Configs=configs}
                }); // 3 partitions and 1 replication factor for the topic
                Console.WriteLine($"Topic({topicName}) is created.");
            }
            else
            {
                Console.WriteLine($"Topic({topicName}) already exists.");
            }

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
    internal async Task SendSimpleMessageWithIntKey(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094"
        };

        using var producer = new ProducerBuilder<int, string>(config).Build();

        foreach (var item in Enumerable.Range(1, 100))
        {
            var message = new Message<int, string>()
            {
                Value = $"Message(use case - 2){item}",
                Key = item
            };

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("---------------------------------");
            await Task.Delay(10);
        }
    }
    internal async Task SendComplexMessageWithIntKey(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094"
        };

        // We use the CustomValueSerializer class to serialize the OrderCreatedEvent object. We use the SetValueSerializer method to set the serializer for the value of the message.
        using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
            .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
            .Build();

        foreach (var item in Enumerable.Range(1, 100))
        {
            // once we created a record object, we cannot change its properties. It is immutable. That is why we use the "with" keyword to create a new object with the new values that has different reference on the memory from the original object.
            var orderCreatedEvent = new OrderCreatedEvent()
            { OrderCode = Guid.NewGuid().ToString(), TotalPrice = item * 200, UserId = item };


            var message = new Message<int, OrderCreatedEvent>()
            {
                Value = orderCreatedEvent,
                Key = item
            };

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("---------------------------------");
            await Task.Delay(10);
        }
    }
    internal async Task SendComplexMessageWithComplexKey(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094"
        };

        // We use the CustomValueSerializer class to serialize the OrderCreatedEvent object. We use the SetValueSerializer method to set the serializer for the value of the message.
        using var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(config)
            .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
            .SetKeySerializer(new CustomKeySerializer<MessageKey>())
            .Build();

        foreach (var item in Enumerable.Range(1, 100))
        {
            // once we created a record object, we cannot change its properties. It is immutable. That is why we use the "with" keyword to create a new object with the new values that has different reference on the memory from the original object.
            var orderCreatedEvent = new OrderCreatedEvent()
            { OrderCode = Guid.NewGuid().ToString(), TotalPrice = item * 200, UserId = item };


            var message = new Message<MessageKey, OrderCreatedEvent>()
            {
                Value = orderCreatedEvent,
                Key = new MessageKey("key1 value", "key2 value")
            };

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("---------------------------------");
            await Task.Delay(10);
        }
    }
    internal async Task SendComplexMessageWithIntKeyAndHeader(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094"
        };

        // We use the CustomValueSerializer class to serialize the OrderCreatedEvent object. We use the SetValueSerializer method to set the serializer for the value of the message.
        using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
            .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
            .Build();

        foreach (var item in Enumerable.Range(1, 3))
        {
            // once we created a record object, we cannot change its properties. It is immutable. That is why we use the "with" keyword to create a new object with the new values that has different reference on the memory from the original object.
            var orderCreatedEvent = new OrderCreatedEvent()
            { OrderCode = Guid.NewGuid().ToString(), TotalPrice = item * 200, UserId = item };

            var header = new Headers
            {
                {"correlationId", Encoding.UTF8.GetBytes("123") },
                {"version", Encoding.UTF8.GetBytes("v1") }
            };

            var message = new Message<int, OrderCreatedEvent>()
            {
                Value = orderCreatedEvent,
                Key = item,
                Headers = header
            };

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("---------------------------------");
            await Task.Delay(10);
        }
    }
    internal async Task SendMessageWithTimestamp(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094"
        };

        // We use the CustomValueSerializer class to serialize the OrderCreatedEvent object. We use the SetValueSerializer method to set the serializer for the value of the message.
        using var producer = new ProducerBuilder<MessageKey, OrderCreatedEvent>(config)
            .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
            .SetKeySerializer(new CustomKeySerializer<MessageKey>())
            .Build();

        foreach (var item in Enumerable.Range(1, 3))
        {
            // once we created a record object, we cannot change its properties. It is immutable. That is why we use the "with" keyword to create a new object with the new values that has different reference on the memory from the original object.
            var orderCreatedEvent = new OrderCreatedEvent()
            { OrderCode = Guid.NewGuid().ToString(), TotalPrice = item * 200, UserId = item };


            var message = new Message<MessageKey, OrderCreatedEvent>()
            {
                Value = orderCreatedEvent,
                Key = new MessageKey("key1 value", "key2 value"),
                // We can set the timestamp of the message . If we don't set it, Kafka will set it automatically.
                //Timestamp = new Timestamp(new DateTime(2012, 02, 02)) 
            };

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("---------------------------------");
            await Task.Delay(10);
        }
    }
    internal async Task SendMessageToSpecifiedPartition(string topicName)
    {
        var config = new ProducerConfig()
        {
            BootstrapServers = "localhost:9094"
        };

        // We use the CustomValueSerializer class to serialize the OrderCreatedEvent object. We use the SetValueSerializer method to set the serializer for the value of the message.
        using var producer = new ProducerBuilder<Null, string>(config).Build();

        foreach (var item in Enumerable.Range(1, 10))
        {
            var message = new Message<Null, string> { Value = $"Message {item}" };


            // We can send the message to the specified partion. In this example, we send the message to the partition 4.
            var topicPartition = new TopicPartition(topicName, new Partition(4));

            var result = await producer.ProduceAsync(topicPartition, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("---------------------------------");
            await Task.Delay(10);
        }
    }
    internal async Task SendMessageWithAck(string topicName)
    {
        // Acknowledgement is used to confirm that the message is received by the broker. We can set the Acknowledgement type in the ProducerConfig. There are 3 types of Acknowledgement: None, Leader and All. All means that the message is received by all replicas. Leader means that the message is received by the leader replice. None means that the message is not received by any replica. None is low latency option but it is not reliable. All is the most reliable option but it is slow. Leader is the middle option between None and All. 
        // We should set the Acknowledgement type to All in such cases like financial transactions, orders, etc. 
        // We can set it to Leader in such cases like logging, monitoring, sending notifications or Welcome emails, etc.
        // We can set it to None in such cases like sending telemetry data etc.
        var config = new ProducerConfig() { BootstrapServers = "localhost:9094", Acks = Acks.All };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        foreach (var item in Enumerable.Range(1, 10))
        {
            var message = new Message<Null, string> { Value = $"Message {item}" };


            // We can send the message to the specified partion. In this example, we send the message to the partition 4.
            var topicPartition = new TopicPartition(topicName, new Partition(4));

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("---------------------------------");
            await Task.Delay(10);
        }
    }
    internal async Task SendMessageToCluster(string topicName)
    {

        var config = new ProducerConfig() { BootstrapServers = "localhost:7000,localhost:7001,localhost:7002", Acks = Acks.All };


        using var producer = new ProducerBuilder<Null, string>(config).Build();

        foreach (var item in Enumerable.Range(1, 20))
        {
            var message = new Message<Null, string> { Value = $"Message {item}" };

            var result = await producer.ProduceAsync(topicName, message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }

            Console.WriteLine("---------------------------------");
            await Task.Delay(10);
        }
    }
    internal async Task SendMessageWithRetryToCluster(string topicName)
    {
        // When we send a message to the Kafka cluster, the message is sent to the leader replica. If the leader replica is not available, the message is not sent to the broker. In such cases, we can set the retry for the message. The default re
        var config = new ProducerConfig() 
        {
            BootstrapServers = "localhost:7000,localhost:7001,localhost:7002", 
            Acks = Acks.All,
            //MessageSendMaxRetries = 5, // This is the number of retries for the message. If the message is not sent to the broker, it will be retried 5 times. But this is not the suggested way to set the retry. 
            //RetryBackoffMs = 1000, // This is the time between the retries.
            //RetryBackoffMaxMs = 5000, // This is the maximum time between the retries.
            MessageTimeoutMs = 5000
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        var message = new Message<Null, string>()
        { 
            Value = $"Message 1" 
        };

        var result = await producer.ProduceAsync(topicName, message);

        foreach (var propertyInfo in result.GetType().GetProperties())
        {
            Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
        }

        Console.WriteLine("---------------------------------");
    }
}
