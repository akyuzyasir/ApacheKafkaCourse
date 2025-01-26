﻿using Confluent.Kafka;
using Kafka.Consumer.Events;
using System.Text;

namespace Kafka.Consumer
{
    internal class KafkaConsumerService
    {
        private readonly string _bootstrapServers = "localhost:9094";
        internal async Task ConsumeSimpleMessageWithNullKey(string topicName)
        {
            // We check whether the topicName is null, empty, or whitespace. If it is, we throw an exception to inform the user.
            if (string.IsNullOrWhiteSpace(topicName))
            {
                throw new ArgumentException("Topic name cannot be null, empty, or whitespace.", nameof(topicName));
            }

            if (!await TopicExists(topicName))
            {
                Console.WriteLine($"Error: Topic '{topicName}' does not exist on the Kafka server.");
                return;
            }
            var config = new ConsumerConfig()
            {
                BootstrapServers = _bootstrapServers,
                GroupId = "use-case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Latest //If We set the offset to the earliest so that we can consume all the messages in the topic. But if we set it to the latest, we will consume only the new messages that will be produced after the consumer is started.
            };
            // We use the same pattern for the consumer as we did for the producer
            var consumer = new ConsumerBuilder<Null, string>(config).Build();

            consumer.Subscribe(topicName);

            while (true)
            {
                // Consume method does not pull the messages every time it is called. It pulls the messages from the broker and stores them in the consumer object. And then we can consume them one by one. When there is no message left, it goes to the broker and pulls the messages again.
                // when there is no message, it waits for the message for the given time in milliseconds. If there is no message in the given time, it returns null.
                var consumeResult = consumer.Consume(5000);

                // We check whether the consumeResult is null or not. If it is not null, we write the message to the console.
                if (consumeResult != null)
                {
                    Console.WriteLine($"gelen mesaj : {consumeResult.Message.Value}");
                }
                await Task.Delay(1000);
            }
        }
        internal async Task ConsumeSimpleMessageWithIntKey(string topicName)
        {
            // We check whether the topicName is null, empty, or whitespace. If it is, we throw an exception to inform the user.
            if (string.IsNullOrWhiteSpace(topicName))
            {
                throw new ArgumentException("Topic name cannot be null, empty, or whitespace.", nameof(topicName));
            }

            if (!await TopicExists(topicName))
            {
                Console.WriteLine($"Error: Topic '{topicName}' does not exist on the Kafka server.");
                return;
            }
            var config = new ConsumerConfig()
            {
                BootstrapServers = _bootstrapServers,
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest //If We set the offset to the earliest so that we can consume all the messages in the topic. But if we set it to the latest, we will consume only the new messages that will be produced after the consumer is started.
            };

            // We use the same pattern for the consumer as we did for the producer
            var consumer = new ConsumerBuilder<int, string>(config).Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                // Consume method does not pull the messages every time it is called. It pulls the messages from the broker and stores them in the consumer object. And then we can consume them one by one. When there is no message left, it goes to the broker and pulls the messages again.
                // when there is no message, it waits for the message for the given time in milliseconds. If there is no message in the given time, it returns null.
                var consumeResult = consumer.Consume(5000);

                // We check whether the consumeResult is null or not. If it is not null, we write the message to the console.
                if (consumeResult != null)
                {
                    Console.WriteLine($"gelen mesaj : Key={consumeResult.Message.Key} Value={consumeResult.Message.Value}");
                }
                await Task.Delay(20);
            }
        }
        internal async Task ConsumeComplexMessageWithIntKey(string topicName)
        {
            // We check whether the topicName is null, empty, or whitespace. If it is, we throw an exception to inform the user.
            if (string.IsNullOrWhiteSpace(topicName))
            {
                throw new ArgumentException("Topic name cannot be null, empty, or whitespace.", nameof(topicName));
            }

            if (!await TopicExists(topicName))
            {
                Console.WriteLine($"Error: Topic '{topicName}' does not exist on the Kafka server.");
                return;
            }
            var config = new ConsumerConfig()
            {
                BootstrapServers = _bootstrapServers,
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest //If We set the offset to the earliest so that we can consume all the messages in the topic. But if we set it to the latest, we will consume only the new messages that will be produced after the consumer is started.
            };

            
            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config).SetValueDeserializer(new CustomValueDesirializer<OrderCreatedEvent>()).Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                // Consume method does not pull the messages every time it is called. It pulls the messages from the broker and stores them in the consumer object. And then we can consume them one by one. When there is no message left, it goes to the broker and pulls the messages again.
                // when there is no message, it waits for the message for the given time in milliseconds. If there is no message in the given time, it returns null.
                var consumeResult = consumer.Consume(5000);

                // We check whether the consumeResult is null or not. If it is not null, we write the message to the console.
                if (consumeResult != null)
                {
                    var orderCreatedEvent = consumeResult.Message.Value;

                    Console.WriteLine($"received message : {orderCreatedEvent.UserId} - {orderCreatedEvent.OrderCode} - {orderCreatedEvent.TotalPrice}");
                }
                await Task.Delay(10);
            }
        }
        internal async Task ConsumeComplexMessageWithComplexKey(string topicName)
        {
            // We check whether the topicName is null, empty, or whitespace. If it is, we throw an exception to inform the user.
            if (string.IsNullOrWhiteSpace(topicName))
            {
                throw new ArgumentException("Topic name cannot be null, empty, or whitespace.", nameof(topicName));
            }

            if (!await TopicExists(topicName))
            {
                Console.WriteLine($"Error: Topic '{topicName}' does not exist on the Kafka server.");
                return;
            }
            var config = new ConsumerConfig()
            {
                BootstrapServers = _bootstrapServers,
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest //If We set the offset to the earliest so that we can consume all the messages in the topic. But if we set it to the latest, we will consume only the new messages that will be produced after the consumer is started.
            };


            var consumer = new ConsumerBuilder<MessageKey, OrderCreatedEvent>(config)
                .SetValueDeserializer(new CustomValueDesirializer<OrderCreatedEvent>())
                .SetKeyDeserializer(new CustomKeyDesirializer<MessageKey>())
                .Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                // Consume method does not pull the messages every time it is called. It pulls the messages from the broker and stores them in the consumer object. And then we can consume them one by one. When there is no message left, it goes to the broker and pulls the messages again.
                // when there is no message, it waits for the message for the given time in milliseconds. If there is no message in the given time, it returns null.
                var consumeResult = consumer.Consume(5000);

                // We check whether the consumeResult is null or not. If it is not null, we write the message to the console.
                if (consumeResult != null)
                {
                    var messageKey = consumeResult.Message.Key;

                    Console.WriteLine($"recieved message(key) => Key1={messageKey.Key1} Key2={messageKey.Key2}");

                    var orderCreatedEvent = consumeResult.Message.Value;

                    Console.WriteLine($"received message(value)=> {orderCreatedEvent.UserId} - {orderCreatedEvent.OrderCode} - {orderCreatedEvent.TotalPrice}");
                }
                await Task.Delay(10);
            }
        }

        internal async Task ConsumeComplexMessageWithIntKeyAndHeader(string topicName)
        {
            // We check whether the topicName is null, empty, or whitespace. If it is, we throw an exception to inform the user.
            if (string.IsNullOrWhiteSpace(topicName))
            {
                throw new ArgumentException("Topic name cannot be null, empty, or whitespace.", nameof(topicName));
            }

            if (!await TopicExists(topicName))
            {
                Console.WriteLine($"Error: Topic '{topicName}' does not exist on the Kafka server.");
                return;
            }
            var config = new ConsumerConfig()
            {
                BootstrapServers = _bootstrapServers,
                GroupId = "use-case-2-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest //If We set the offset to the earliest so that we can consume all the messages in the topic. But if we set it to the latest, we will consume only the new messages that will be produced after the consumer is started.
            };


            var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config).SetValueDeserializer(new CustomValueDesirializer<OrderCreatedEvent>()).Build();
            consumer.Subscribe(topicName);

            while (true)
            {
                // Consume method does not pull the messages every time it is called. It pulls the messages from the broker and stores them in the consumer object. And then we can consume them one by one. When there is no message left, it goes to the broker and pulls the messages again.
                // when there is no message, it waits for the message for the given time in milliseconds. If there is no message in the given time, it returns null.
                var consumeResult = consumer.Consume(5000);

                // We check whether the consumeResult is null or not. If it is not null, we write the message to the console.
                if (consumeResult != null)
                {
                    // We can get the values of the headers by using the name of the header. 
                    var correlationId = Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("correlationId"));

                    var version = Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("version"));

                    // We can also get the values of the headers by using the index of the header.
                    //var correlationId2 = consumeResult.Message.Headers[0].GetValueBytes();

                    //var version2 = consumeResult.Message.Headers[1].GetValueBytes();


                    var orderCreatedEvent = consumeResult.Message.Value;

                    Console.WriteLine($"received message : {orderCreatedEvent.UserId} - {orderCreatedEvent.OrderCode} - {orderCreatedEvent.TotalPrice}");
                }
                await Task.Delay(10);
            }
        }


        private async Task<bool> TopicExists(string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _bootstrapServers }).Build();

            try
            {
                // Get the metadata for all topics
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));

                // Check if the provided topic name exists in the metadata
                return metadata.Topics.Any(t => t.Topic == topicName);
            }
            catch (KafkaException ex)
            {
                Console.WriteLine($"Error fetching topic metadata: {ex.Message}");
                return false;
            }
        }

    }
}
