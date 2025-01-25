using Confluent.Kafka.Admin;
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Consumer
{
    internal class KafkaService
    {

        internal async Task ConsumeSimpleMessageWithNullKey(string topicName)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "localhost:9094",
                GroupId = "use-case-1-group-1",
                AutoOffsetReset = AutoOffsetReset.Earliest
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
    }
}
