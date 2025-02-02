namespace Shared.Events;

public class BusConstants
{
    // We defined the topic group id as a constant in order to prevent typos and to be able to use it in multiple places.
    public const string OrderCreatedEventGroupId= "group-stock";
    public const string OrderCreatedEventTopicName = "order-created-event";

}
