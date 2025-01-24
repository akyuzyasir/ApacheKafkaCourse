using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaComponents
{
    internal class P10ConsumerGroup
    {
        // Consumer Group
        // Consumer Group is a group of consumers that listens to the same topic. Let's assume we have a topic with 3 partitions, a group named "group-1" with 3 consumers and another consumer group named "group-2" with 2 consumers. 
        /*
            Advantages of Consumer Groups
            Horizontal Scaling: Add consumers to the group to handle higher workloads.
            Fault Tolerance: Other consumers in the group take over partitions of a failed consumer.
            Dynamic Membership: Consumers can dynamically join or leave a group without affecting the overall process.
         
         */
    }
}
