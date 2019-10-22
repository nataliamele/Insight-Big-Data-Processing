#Create the topic with replication factor, number of partitions and topic name as arguments to the command

bin/kafka-topics.sh --zookeeper zookeeper1:2181,zookeeper2:2181,zookeeper3:2181/kafka --create --topic aot-stream --replication-factor 3 --partitions 8
