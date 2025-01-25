using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Producer.Events
{
    // Records and classes are both reference types in C#. Records are designed to facilitate immutability and provide built-in value-based equality for their properties, making them suitable for scenarios where data comparison is required. Classes, by default, use reference-based equality but can be customized for value-based equality. Comparing two objects of type record or class is not inherently faster or slower—it depends on how equality is implemented.
    internal record OrderCreatedEvent
    {
        // The init accessor is used to set the value of a property. It is used in the context of object initializers.
        public string OrderCode { get; init; } = default!; // The "default!" expression is used to suppress the nullable warning. It tells the compiler that the value will not be null.
        public decimal TotalPrice { get; init; }
        public int UserId { get; init; }
    }
}
