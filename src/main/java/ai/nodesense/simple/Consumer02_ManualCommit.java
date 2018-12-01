package ai.nodesense.simple;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import java.util.Arrays;
import java.util.Properties;

public class Consumer02_ManualCommit {
    public static Properties getConfig(String group, String servers) {

        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", group);

        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        // For manual commit
        props.put("enable.auto.commit", "false");
        //props.put("auto.commit.interval.ms", "1000");

        return props;
    }

    public static void simpleConsumer(String topic, String group, String servers) {

        Properties props = getConfig(group, servers);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            // Notes: If records length > 0, then commit is send to broker.

            // Process the messages here,

            for (ConsumerRecord<String, String> record : records) {
                int partition = record.partition();
                int offset = record.partition();
                String key = record.key();
                String value = record.value();
                TimestampType timestampType = record.timestampType();
                long timeStamp = record.timestamp();


                Headers headers = record.headers();

                System.out.printf("partition =%d, offset = %d,  key = %s, value = %s timestamp type = %s timestamp=%d\n",
                        partition, offset, key, value, timestampType, timeStamp);


            }

            // commit offet after messages are processed
            // if any problem in messages, don't commit
            // then messages shall re-read
            //consumer.commitSync();

            // also commitAsync for non-blocking commit

        }
    }


    public static void main(String[] args) throws Exception {
        String topic = "messages2";
        String group = "consumer_group_6";
        String servers = "broker:9092";

        simpleConsumer(topic, group, servers);
    }
}