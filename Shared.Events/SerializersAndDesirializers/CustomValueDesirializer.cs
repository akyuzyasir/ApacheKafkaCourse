using Confluent.Kafka;
using System.Text.Json;

namespace Shared.Events.SerializersAndDesirializers;

public class CustomValueDesirializer<T> : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        // We convert the byte array to a string and then deserialize it to the T type.
        return JsonSerializer.Deserialize<T>(data)!; // The "!" expression is used to suppress the nullable warning. It tells the compiler that the value will not be null.
    }
}
