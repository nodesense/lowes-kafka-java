package ai.nodesense.streaming;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PageViewRegionExample {

    public static void main(final String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "pageview-region-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "pageview-region-example-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Where to find the Confluent schema registry instance(s)
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final StreamsBuilder builder = new StreamsBuilder();

        // Create a stream of page view events from the PageViews topic, where the key of
        // a record is assumed to be null and the value an Avro GenericRecord
        // that represents the full details of the page view event. See `pageview.avsc` under
        // `src/main/avro/` for the corresponding Avro schema.
        final KStream<String, GenericRecord> views = builder.stream("PageViews");

        // Create a keyed stream of page view events from the PageViews stream,
        // by extracting the user id (String) from the Avro value
        final KStream<String, GenericRecord> viewsByUser = views.map(new KeyValueMapper<String, GenericRecord, KeyValue<String, GenericRecord>>() {
            @Override
            public KeyValue<String, GenericRecord> apply(final String dummy, final GenericRecord record) {
                return new KeyValue<>(record.get("user").toString(), record);
            }
        });

        // Create a changelog stream for user profiles from the UserProfiles topic,
        // where the key of a record is assumed to be the user id (String) and its value
        // an Avro GenericRecord.  See `userprofile.avsc` under `src/main/avro/` for the
        // corresponding Avro schema.
        final KTable<String, GenericRecord> userProfiles = builder.table("UserProfiles");

        // Create a changelog stream as a projection of the value to the region attribute only
        final KTable<String, String> userRegions = userProfiles.mapValues(new ValueMapper<GenericRecord, String>() {
            @Override
            public String apply(final GenericRecord record) {
                return record.get("region").toString();
            }
        });

        // We must specify the Avro schemas for all intermediate (Avro) classes, if any.
        // In this example, we want to create an intermediate GenericRecord to hold the view region
        // (see below).
        final InputStream
                pageViewRegionSchema =
                PageViewRegionLambdaExample.class.getClassLoader()
                        .getResourceAsStream("avro/pageviewregion.avsc");
        final Schema schema = new Schema.Parser().parse(pageViewRegionSchema);

        final KTable<Windowed<String>, Long> viewsByRegion = viewsByUser
                .leftJoin(userRegions, new ValueJoiner<GenericRecord, String, GenericRecord>() {
                    @Override
                    public GenericRecord apply(final GenericRecord view, final String region) {
                        final GenericRecord viewRegion = new GenericData.Record(schema);
                        viewRegion.put("user", view.get("user"));
                        viewRegion.put("page", view.get("page"));
                        viewRegion.put("region", region);
                        return viewRegion;
                    }
                })
                .map(new KeyValueMapper<String, GenericRecord, KeyValue<String, GenericRecord>>() {
                    @Override
                    public KeyValue<String, GenericRecord> apply(final String user, final GenericRecord viewRegion) {
                        return new KeyValue<>(viewRegion.get("region").toString(), viewRegion);
                    }
                })
                // count views by user, using hopping windows of size 5 minutes that advance every 1 minute
                .groupByKey() // no need to specify explicit serdes because the resulting key and value types match our default serde settings
                .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(5)).advanceBy(TimeUnit.MINUTES.toMillis(1)))
                .count();

        // Note: The following operations would NOT be needed for the actual pageview-by-region
        // computation, which would normally stop at `count` above.  We use the operations
        // below only to "massage" the output data so it is easier to inspect on the console via
        // kafka-console-consumer.
        final KStream<String, Long> viewsByRegionForConsole = viewsByRegion
                // get rid of windows (and the underlying KTable) by transforming the KTable to a KStream
                // and by also converting the record key from type `Windowed<String>` (which
                // kafka-console-consumer can't print to console out-of-the-box) to `String`
                .toStream(new KeyValueMapper<Windowed<String>, Long, String>() {
                    @Override
                    public String apply(final Windowed<String> windowedRegion, final Long count) {
                        return windowedRegion.toString();
                    }
                });

        // write to the result topic
        viewsByRegionForConsole.to("PageViewsByRegion", Produced.with(stringSerde, longSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
            }
        }));
    }

}