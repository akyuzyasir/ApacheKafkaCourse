using Confluent.Kafka.Admin;
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kafka.Producer.Events;
using Kafka.Product.Events;

namespace Kafka.Producer
{
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

                if (!topicExists)
                {
                    await adminClient.CreateTopicsAsync(new[]
                    {
                        new TopicSpecification(){ Name= topicName, NumPartitions = 3, ReplicationFactor = 1 }
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
                { OrderCode = Guid.NewGuid().ToString(), TotalPrice = item * 200, UserId=item};


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

    }
}
