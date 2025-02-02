using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Shared.Events
{
    public class CustomValueSerializer<T> : ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            // We serialize T type data to JSON format and then convert it to a byte array.
            return Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data, typeof(T)));
        }
    }
}
