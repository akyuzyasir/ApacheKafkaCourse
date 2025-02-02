namespace Order.API.Services;

public class BusConstants
{
    // We defined the topic name as a constant in order to prevent typos and to be able to use it in multiple places.
    public const string OrderCreatedEventTopicName = "order-created-event";
}
