package ai.nodesense.products;



import ai.nodesense.models.ProductOrder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

//kafka-topics --create --topic product.orders --zookeeper localhost:2181 --partitions 4 --replication-factor 1


class ProductOrderGenerator {


    static Random random = new Random();


    static int[] categories = {1, 2, 3, 4};

    static int[] customerIds = {1000, 2000, 3000, 4000, 5000, 6000};

    static String[] customerNames = {"Krish", "Gayathri", "Nila", "Venkat", "Hari", "Ravi"};

    static String[] productNames = {"iPhone", "Moto G", "One Plus", "Samsung Nexus", "Google Pixel", "Vivo"};
    static int[] productIds = {11, 22, 33, 44, 55, 66};


    static int[] stateIds = {10, 20, 30, 40};


    public static ProductOrder getNext() {

        int categoryId = customerIds[random.nextInt(customerIds.length)];
        int stateId = customerIds[random.nextInt(customerIds.length)];

        int customerId = random.nextInt(100);

        int productIndex = random.nextInt(productIds.length);

        int productId = productIds[productIndex];

        String productName = productNames[productIndex];

        int id = random.nextInt(1000000);


        ProductOrder order = new ProductOrder();

        order.setId(id);
        order.setCustomerId(customerId);
        order.setName(productName);
        order.setQty(random.nextInt(5) + 1);
        order.setCategory(categoryId);
        order.setState(stateId);
        order.setTimestamp(System.currentTimeMillis());

        return order;
    }

}

public class ProductOrderProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        long events = 10;
        String schemaUrl = "http://localhost:8081";

        Properties props = new Properties();
        // hardcoding the Kafka server URI for this example
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", schemaUrl);


        String topic = "product.orders";

        Producer<String, ProductOrder> producer = new KafkaProducer<String, ProductOrder>(props);

        Random rnd = new Random();
        for (long nEvents = 0; nEvents < events; nEvents++) {
            ProductOrder productOrder = ProductOrderGenerator.getNext();

            // Using IP as key, so events from same IP will go to same partition
            ProducerRecord<String, ProductOrder> record = new ProducerRecord<String, ProductOrder>(topic, productOrder.getId().toString(), productOrder);
            producer.send(record).get();
            System.out.println("Sent ProductOrder" + productOrder.getId());
            Thread.sleep(1000);
        }

    }
}
