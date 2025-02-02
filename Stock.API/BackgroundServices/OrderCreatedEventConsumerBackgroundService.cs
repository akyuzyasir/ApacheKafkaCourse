using Confluent.Kafka;
using Shared.Events;
using Shared.Events.Events;
using Shared.Events.SerializersAndDesirializers;
using Stock.API.Services;

namespace Stock.API.BackgroundServices
{
    public class OrderCreatedEventConsumerBackgroundService(IBus bus, ILogger<OrderCreatedEventConsumerBackgroundService> logger) : BackgroundService
    {
        private IConsumer<string,OrderCreatedEvent>? _consumer;
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _consumer = new ConsumerBuilder<string, OrderCreatedEvent>(bus.GetConsumerConfig(BusConstants.OrderCreatedEventGroupId)).SetValueDeserializer(new CustomValueDesirializer<OrderCreatedEvent>()).Build();
            _consumer.Subscribe(BusConstants.OrderCreatedEventTopicName);

            // When cancellation request of program called the loop will stop
            while (!stoppingToken.IsCancellationRequested)
            {
                var consumeResult = _consumer!.Consume(5000);

                if(consumeResult != null)
                {
                    try
                    {
                        var orderCreatedEvent = consumeResult.Message.Value;

                        // stocks are updated

                        Console.WriteLine($"user id : {orderCreatedEvent.UserId}, order code : {orderCreatedEvent.OrderCode}, total price : {orderCreatedEvent.TotalPrice}");
                         
                        _consumer.Commit(consumeResult);
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e.Message);
                    }
                }
                await Task.Delay(10, stoppingToken);
            }
        }
    }
}
