using Shared.Events;
using Shared.Events.Events;

namespace Order.API.Services;

public class OrderService(IBus bus)
{
    public async Task<bool> Create(OrderCreatedRequestDto request)
    {
        // save order to database. Since this is a demo project, we don't have a database. So we will skip this step.

        var orderCode = Guid.NewGuid().ToString();
        // We created a new Guid as the order code. And passed the UserId and TotalPrice from the request to the OrderCreatedEvent.
        var orderCreatedEvent = new OrderCreatedEvent(orderCode, request.UserId, request.TotalPrice);

        return await bus.Publish(orderCode, orderCreatedEvent, BusConstants.OrderCreatedEventTopicName);
    }
}
