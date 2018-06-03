package ai.nodesense.avro;

import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import ai.nodesense.models.LogLine;


public class AvroSimpleConsumer {
    public static void main(String[] args) throws Exception {


        String topic = "clicks";
        String group = "simple2";
        String schemaUrl = "http://localhost:8081";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", group);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");


        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");


        props.put("schema.registry.url", schemaUrl);

        KafkaConsumer<String, LogLine> consumer = new KafkaConsumer<String, LogLine>(props);

        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);
        int i = 0;

        while (true) {
            ConsumerRecords<String, LogLine> records = consumer.poll(100);
            for (ConsumerRecord<String, LogLine> record : records)
                System.out.printf("partition =%d, offset = %d,  key = %s, value = %s\n",
                        record.partition(),  record.offset(), record.key(), record.value());
        }
    }
}