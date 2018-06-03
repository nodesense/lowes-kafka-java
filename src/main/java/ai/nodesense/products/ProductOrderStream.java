package ai.nodesense.products;



import ai.nodesense.models.ProductOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class ProductOrderStream {

    public static void main(final String[] args) throws Exception {
        System.out.println("Running ProductOrder Stream");

        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "product-orders-stream");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "product-orders-stream-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);

        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        String schemaUrl = "http://localhost:8081";

        streamsConfiguration.put("schema.registry.url", schemaUrl);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // In the subsequent lines we define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, ProductOrder> productOrders = builder.stream("product.orders");

        productOrders.foreach(new ForeachAction<String, ProductOrder>() {
            @Override
            public void apply(String key, ProductOrder value) {
                System.out.println("yeah " + key + " Value is  " + value );
            }
        });

        KStream<String, Integer> productOrdersQty = productOrders.mapValues(value -> value.getQty());

        KStream<String, Integer> transformed = productOrders.map(
                (key, value) -> KeyValue.pair(value.getName().toString(), value.getQty()));

        transformed.foreach(new ForeachAction<String, Integer>() {
            @Override
            public void apply(String key, Integer value) {
                System.out.println("Product to Qty  " + key + " Value is  " + value );
            }
        });

        //productOrders.print(Printed.toSysOut());

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}