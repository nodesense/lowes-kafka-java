package ai.nodesense.simple;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


import org.apache.kafka.common.errors.WakeupException;
import java.util.*;

import java.util.Arrays;
import java.util.Properties;

public class RebalancedSimpleConsumer {
    public static Properties getConfig(String group, String servers) {

        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", group);

        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }

    public static void rebalancedConsumer(String topic, String group, String servers) {

        Properties props = getConfig(group, servers);

        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");


        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client1234567");


        long startingOffset = 0;

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //consumer.subscribe(Arrays.asList(topic));

        List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        System.out.println("Partitions " + partitions);


        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

                System.out.printf("%s topic-partitions are revoked from this consumer\n", Arrays.toString(partitions.toArray()));
            }
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.printf("%s topic-partitions are assigned to this consumer\n", Arrays.toString(partitions.toArray()));
                Iterator<TopicPartition> topicPartitionIterator = partitions.iterator();
                while(topicPartitionIterator.hasNext()){
                    TopicPartition topicPartition = topicPartitionIterator.next();

                    System.out.println("Current offset is " + consumer.position(topicPartition) + " committed offset is ->" + consumer.committed(topicPartition) );
                    if(startingOffset == -2) {
                        System.out.println("Leaving it alone");
                    }else if(startingOffset ==0){
                        System.out.println("Setting offset to begining");
                        consumer.seekToBeginning(Arrays.asList(topicPartition));
                    }else if(startingOffset == -1){
                        System.out.println("Setting it to the end ");
                        consumer.seekToEnd(Arrays.asList(topicPartition));
                    }else {
                        System.out.println("Resetting offset to " + startingOffset);
                        consumer.seek(topicPartition, startingOffset);
                    }
                }
            }
        });

        System.out.println("Subscribed to topic " + topic);


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            if (records.count() > 0) {
                System.out.println("Total Records " + records.count());
            } else {
                continue;
            }

            for (ConsumerRecord<String, String> record : records)
                System.out.printf("partition =%d, offset = %d,  key = %s, value = %s\n",
                        record.partition(),  record.offset(), record.key(), record.value());


        }
    }



    public static void main(String[] args) throws Exception {
        String topic = "test3";
        String group = "simple2";
        String servers = "localhost:9092";


        rebalancedConsumer(topic, group, servers);






    }
}