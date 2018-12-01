package ai.nodesense.simple;//import util.properties packages
import java.util.Properties;
import java.util.Random;
//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.Headers;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

//Create java class named “ai.nodesense.simple.SimpleProducer”
public class  SimpleProducer {

    public static Properties getConfig() {
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "broker:9092");

        //Set acknowledgements for producer requests.
        // Ensures that all replicas stored the messages
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

    public static void sendMessages(String topicName) throws Exception {

        // create instance for properties to access producer configs
        Properties props = getConfig();

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        Random r = new Random();
        for(int i = 0; i < 10; i++) {
            String key = "User" + r.nextInt(4);
            String value = "Value " + Integer.toString(i);

            ProducerRecord record = new ProducerRecord<String, String>(topicName,
                    key, value);

            Headers headers = record.headers();
            headers.add("ClientIP", "192.168.1.100".getBytes());
            headers.add("AppName", "App2".getBytes());

            producer.send(record);

            System.out.println("Message sent " + key + "," + value);
            Thread.sleep(1000);
        }
        System.out.println("All Messages sent successfully");
        producer.close();
    }


    public static void sendMessagesSync(String topicName) throws Exception {

        // create instance for properties to access producer configs
        Properties props = getConfig();

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        Random r = new Random();
        long time = System.currentTimeMillis();


        for(int i = 0; i < 10; i++) {
            String key = "User" + r.nextInt(4);
            String value = "Value " + Integer.toString(i);

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,
                    key, value);

            // sync, wait, block current thread
            RecordMetadata metadata = producer.send(record).get();

            System.out.println("Message sent " + key + "," + value);
            long elapsedTime = System.currentTimeMillis() - time;
            System.out.printf("sent record(key=%s value=%s) " +
                            "meta(partition=%d, offset=%d) time=%d\n",
                    record.key(), record.value(), metadata.partition(),
                    metadata.offset(), elapsedTime);


            Thread.sleep(1000);
        }
        System.out.println("All Messages sent successfully");
        producer.flush();
        producer.close();
    }


    public static void sendMessagesAsync(String topicName) throws Exception {

        // create instance for properties to access producer configs
        Properties props = getConfig();

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        Random r = new Random();
        long time = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(10);


        for(int i = 0; i < 10; i++) {
            String key = "User" + r.nextInt(4);
            String value = "Value " + Integer.toString(i);

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,
                    key, value);

            // non-blocking call, no wait
            producer.send(record, (metadata, exception) -> {
                long elapsedTime = System.currentTimeMillis() - time;
                if (metadata != null) {
                    System.out.printf("sent record(key=%s value=%s) " +
                                    "meta(partition=%d, offset=%d) time=%d\n",
                            record.key(), record.value(), metadata.partition(),
                            metadata.offset(), elapsedTime);
                } else {
                    exception.printStackTrace();
                }
                countDownLatch.countDown();
            });



            Thread.sleep(1000);
        }

        countDownLatch.await(25, TimeUnit.SECONDS);


        System.out.println("All Messages sent successfully");
        producer.flush();
        producer.close();
    }


    public static void sendMessagesWithoutKey(String topicName) throws Exception {

        // create instance for properties to access producer configs
        Properties props = getConfig();

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        Random r = new Random();
        for(int i = 0; i < 10; i++) {
            //String key = "User" + r.nextInt(4);
            String value = "Value " + Integer.toString(i);
            producer.send(new ProducerRecord<String, String>(topicName,
                    null, value));
            System.out.println("Message sent keu null"  + "," + value);
            Thread.sleep(1000);
        }
        System.out.println("All Messages sent successfully");
        producer.close();
    }



    public static void sendMessagesWithTimestamp(String topicName) throws Exception {

        // create instance for properties to access producer configs
        Properties props = getConfig();

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        Random r = new Random();
        for(int i = 0; i < 10; i++) {
            String key = "UserTimeout" + r.nextInt(4);
            String value = "Value " + Integer.toString(i);

            //long expiry = System.currentTimeMillis() + 1000 * 10;
            long expiry =  1000 * 10;

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,  (Integer)null, expiry, key, value);


            producer.send(record);

            System.out.println("Message sent "  + key +  "," + value);
            Thread.sleep(1000);
        }
        System.out.println("All Messages sent successfully");
        producer.close();
    }


    public static void sendMessagesCustomPartition(String topicName) throws Exception {

        // create instance for properties to access producer configs
        Properties props = getConfig();

        props.put("partitioner.class",
                "ai.nodesense.partitions.CustomPartitioner");

        Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        Random r = new Random();
        for(int i = 0; i < 10; i++) {
            String key = "User" + r.nextInt(4);
            String value = "Value " + Integer.toString(i);
            producer.send(new ProducerRecord<String, String>(topicName,
                    key, value));
            System.out.println("Message sent " + key + "," + value);
            Thread.sleep(1000);
        }
        System.out.println("All Messages sent successfully");
        producer.close();
    }




    public static void main(String[] args) throws Exception{
        Thread.currentThread().setContextClassLoader(null);

        String topicName = "messages2";

        sendMessages(topicName);
        // sendMessagesSync(topicName);

        //sendMessagesAsync(topicName);

        // sendMessagesWithoutKey(topicName);

        // sendMessagesWithTimestamp(topicName);

        System.out.println("Main Thread ID " + Thread.currentThread().getId());
       // sendMessagesCustomPartition(topicName);
    }
}