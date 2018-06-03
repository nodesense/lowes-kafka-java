package ai.nodesense.json;




import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public class JsonProducer {
    private static final Random RANDOM = new Random();

    public static void main(String... argv) throws Exception {


        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092");


        System.out.printf("Connecting to Kafka on %s\n", properties.getProperty("bootstrap.servers"));

        String serializer = JacksonReadingSerializer.class.getName();

        System.out.println("Value " + serializer);

        properties.put("value.serializer", serializer);

        runProducer(properties, "json-topic");


    }

    public static void runConsumer(Properties properties, String topic) throws Exception {
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        System.out.printf("Running consumer with serializer %s on topic %s\n", properties.getProperty("value.deserializer"), topic);

        KafkaConsumer<String, SensorReading> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, SensorReading> records = consumer.poll(100);
            for (ConsumerRecord<String, SensorReading> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
        }
    }

    public static void runProducer(Properties properties, String topic) throws Exception {
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        System.out.printf("Running producer with serializer %s on topic %s\n", properties.getProperty("value.serializer"), topic);

        Producer<String, SensorReading> producer = new KafkaProducer<>(properties);

        for(int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(topic, Integer.toString(i), randomReading()));
            System.out.println("Send message");
            Thread.sleep(5000);
        }

        producer.close();
    }

    private static SensorReading randomReading() {
        Sensor.Type type = Sensor.Type.values()[RANDOM.nextInt(Sensor.Type.values().length)];
        String id = type.toString().toLowerCase() + "-" + RANDOM.nextInt(10);
        double value = 0;
        switch(type) {
            case HUMIDITY:
                value = RANDOM.nextDouble() * 50.0 + 50.0;
                break;
            case TEMP:
                value = RANDOM.nextDouble() * 15.0 + 15.0;
                break;
            case LIGHT:
                value = RANDOM.nextDouble() * 12000.0; // candelas/m2
                break;
        }
        return new SensorReading(new Sensor(id, type), System.currentTimeMillis(), value);
    }
}