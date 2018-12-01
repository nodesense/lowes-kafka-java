package ai.nodesense.simple;

import java.util.Properties;
import java.util.Arrays;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SimpleConsumer {
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

    public static void simpleConsumer(String topic, String group, String servers, boolean autoCommit, boolean manualCommit) {

        Properties props = getConfig(group, servers);


        if (autoCommit) {
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
        } else {
            props.put("enable.auto.commit", "false");
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {

                // each records needs 2 seconds

                //after 2 seconds, got exception

                System.out.printf("partition =%d, offset = %d,  key = %s, value = %s timestamp type = %s timestamp=%d\n",
                        record.partition(), record.offset(), record.key(), record.value(), record.timestampType(), record.timestamp());

            }

            if (manualCommit) {
                consumer.commitSync();
            }
        }
    }


    public static void simpleConsumerFromBeginning(String topic, String group, String servers, boolean autoCommit, boolean manualCommit) {

        Properties props = getConfig(group, servers);

        // should be random if
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer_client_id");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");




        if (autoCommit) {
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
        } else {
            props.put("enable.auto.commit", "false");
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);


        consumer.poll(0);
        // Now there is heartbeat and consumer is "alive"
        consumer.seekToBeginning(consumer.assignment());


        ConsumerRecords<String, String> records2 = consumer.poll(0);




        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            if (records.count() > 0) {
                System.out.println("Total Records " + records.count());
            } else {
                continue;
            }

            for (ConsumerRecord<String, String> record : records) {

                System.out.printf("partition =%d, offset = %d,  key = %s, value = %s timestamp type = %s timestamp=%d\n",
                        record.partition(), record.offset(), record.key(), record.value(), record.timestampType(), record.timestamp());

            }

            if (manualCommit) {
                consumer.commitSync();
            }
        }
    }





    public static void main(String[] args) throws Exception {
        String topic = "test";
        String group = "messages";
        String servers = "broker:9092";

        // simpleConsumer(topic, group, servers, true, false);

        // SimpleConsumer without AutoCommit
        // no offset is commited to kafka, no manual sync so, messages are reread again
       // simpleConsumer(topic, group, servers, false, false);

        // manually set the commit by callimg commitSync
       // simpleConsumer(topic, group, servers, false, true);

        simpleConsumerFromBeginning(topic, group, servers, false, true);






    }
}