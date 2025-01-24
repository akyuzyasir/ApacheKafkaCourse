using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaComponents
{
    internal class P09ProducerConsumerInteraction
    {
        // How do we interact with Kafka Producer and Consumer?
        // Firstly, the producer serialize the message and send it to the broker. And then broker either sends the message to RAM before saving to disk or directly saves to the disk. Zero Copy technique is used to save the message to the disk. 
        // The consumer reads the message from the broker and deserializes it.
        // Message - Record
        // Message; contains key, value, timestamp, and topic. Key is optional. Value is the actual message. Timestamp is the time when the message is created. Topic is the name of the topic. 
    }
}
