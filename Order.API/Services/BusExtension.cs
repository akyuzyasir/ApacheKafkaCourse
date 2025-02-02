using Shared.Events;
using System.Runtime.CompilerServices;

namespace Order.API.Services
{
    public static class BusExtension
    {
        public static async Task CreateTopicsOrQueuesAsync(this WebApplication app)
        {
            using var scope = app.Services.CreateScope();

            var bus = scope.ServiceProvider.GetRequiredService<IBus>();
            await bus.CreateTopicsOrQueuesAsync([BusConstants.OrderCreatedEventTopicName]);

        }
    }
}
