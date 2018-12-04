# StormStreamKafka2Hdfs

- Create a topic called 'storm101' , with 4 partitions and replication-factor of 2.
- Create a new directory in HDFS named '/kafka_msgs' and set the directory ownership to storm:hadoop
- Deploy the topology using following command:
`storm jar <target directory>/kafka2hdfs-0.0.1-SNAPSHOT.jar com.hwxlabs.storm.StreamKafka2Hdfs.Topology`
-   Produce messages into the kafka topic using the console producer. For example, the following command writes all new lines from ambari-agent log to the topic:
`nohup tail -f /var/log/ambari-agent/ambari-agent.log | /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list <broker>:6667 --topic storm101  &`

