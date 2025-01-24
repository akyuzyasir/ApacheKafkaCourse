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

        #region  Consumer - Offset - Partition 

        // Consumer reads the message from the broker. Offset is the position of the message in the partition. A consumer can read messages from multiple partitions. However, in the case of amount of consumers is more than the partitions, the consumer reads the message from the same partition. But consumers cannot connect to the same partition at the same time. 
        #endregion
    }
}
