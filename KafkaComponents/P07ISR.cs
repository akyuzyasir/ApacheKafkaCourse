using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaComponents;

internal class ISR
{
    // ISR stand for In-Sync Replica
    /*
        * ISR is a list of replicas that are in sync with the leader.
        * In-Sync Replica represents the number of followers and determines which partition to be the leader partition.
        * When a leader fails, the ISR list is used to determine the new leader.
        * Lets say we have 3 replicas, 1 leader and 2 followers. If the leader fails, the follower with the highest ISR will be the new leader. 
        * We can set the minimum ISR to 2, so if the ISR list is less than 2, the partition will not be available. 

     */
}
