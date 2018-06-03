package ai.nodesense.avro;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;


import ai.nodesense.models.LogLine;

import java.util.Date;

class EventGenerator {
    static long numUsers = 10000;
    static long currUser = 1;

    static String[] websites = {"support.html","about.html","foo.html", "bar.html", "home.html", "search.html", "list.html", "help.html", "bar.html", "foo.html"};


    public static LogLine getNext() {
        LogLine event = new LogLine();
        int ip4 =(int) currUser % 256;
        long runtime = new Date().getTime();
        Random r = new Random();
        event.setIp("66.249.1."+ ip4);
        event.setReferrer("www.example.com");
        event.setTimestamp(runtime);
        event.setUrl(websites[r.nextInt(websites.length)]);
        event.setUseragent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.125 Safari/537.36");
        currUser += 1;
        return event;
    }

}

public class AvroProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        if (args.length != 2) {
//            System.out.println("Please provide command line arguments: numEvents schemaRegistryUrl");
//            System.exit(-1);
//        }
//

       // long events = Long.parseLong(args[0]);
       // String schemaUrl = args[1];

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
        // Hard coding topic too.
        String topic = "clicks";

        Producer<String, LogLine> producer = new KafkaProducer<String, LogLine>(props);

        Random rnd = new Random();
        for (long nEvents = 0; nEvents < events; nEvents++) {
            LogLine event = EventGenerator.getNext();

            // Using IP as key, so events from same IP will go to same partition
            ProducerRecord<String, LogLine> record = new ProducerRecord<String, LogLine>(topic, event.getIp().toString(), event);
            producer.send(record).get();
            Thread.sleep(1000);


        }

    }
}
