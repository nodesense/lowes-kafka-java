package ai.nodesense.products;


import java.util.Properties;
import java.util.Arrays;

import ai.nodesense.models.ProductOrder;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class ProductOrderConsumer {
    public static void main(String[] args) throws Exception {


        String topic = "product.orders";
        String group = "ProductOrderGroup";
        String schemaUrl = "http://schema-registry:8081";

        Properties props = new Properties();
        props.put("bootstrap.servers", "broker:9092");
        props.put("group.id", group);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");


        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");


        props.put("schema.registry.url", schemaUrl);

        KafkaConsumer<String, ProductOrder> consumer = new KafkaConsumer<String, ProductOrder>(props);

        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);
        int i = 0;

        while (true) {
            ConsumerRecords<String, ProductOrder> records = consumer.poll(100);

            if (records.count() == 0) {
                // do other tasks
            }

            for (ConsumerRecord<String, ProductOrder> record : records)
                System.out.printf("partition =%d, offset = %d,  key = %s, value = %s\n",
                        record.partition(),  record.offset(), record.key(), record.value());
        }
    }
}