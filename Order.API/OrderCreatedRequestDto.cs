namespace Order.API
{
    public record OrderCreatedRequestDto(string UserId, decimal TotalPrice);
}
