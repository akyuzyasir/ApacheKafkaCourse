using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Introduction;

internal class KafkaComponents
{

    /*
        // Kafka Brokers
        - Kafka Brokers: Kafka brokers are the servers that store and manage the data in Kafka. They are responsible for receiving messages from producers, storing them in topics, and delivering them to consumers. Brokers can be added or removed from a Kafka cluster to scale the system horizontally. Each broker in a Kafka cluster is identified by a unique broker ID and can be configured with multiple partitions to distribute data across the cluster. Brokers communicate with each other to replicate data and maintain fault tolerance in the Kafka cluster. When data is sharded across multiple brokers, it can be replicated to ensure durability and availability.
        - Kafka Cluster: A Kafka cluster is a group of Kafka brokers that work together to store and manage data. It provides fault tolerance and scalability by distributing data across multiple brokers.
        - Kafka Topics: Kafka topics are the categories to which messages are published by producers. Topics are divided into partitions, which are distributed across brokers in the Kafka cluster.
        - Kafka Partitions: Kafka partitions are the units of parallelism in Kafka. Each partition is an ordered sequence of messages that is stored on a broker. Partitions allow messages to be distributed across multiple brokers in the Kafka cluster. Each partition has a leader broker that is responsible for handling read and write requests for the partition. Partitions can be replicated to provide fault tolerance and high availability.
        - Kafka Producers: Kafka producers are the clients that publish messages to Kafka topics. They are responsible for sending messages to Kafka brokers and specifying the topic to which the messages should be published.
        - Kafka Consumers: Kafka consumers are the clients that subscribe to Kafka topics and consume messages from them. They are responsible for reading messages from Kafka brokers and processing them.
        - Kafka Zookeeper: Kafka uses Zookeeper to manage the metadata of the Kafka cluster. Zookeeper stores information about the brokers, topics, partitions, and consumers in the Kafka cluster.

     */
}
