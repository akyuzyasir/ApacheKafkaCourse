using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaComponents
{
    internal class P08TopicCreate
    {
        // How do we create a topic in Kafka?
        // First we use Confluent.Kafka NuGet package
        // Partition Count, ISR Count and Replication Factor are the key parameters
        // Read and write processes only happen in the leader partition
    }
}
