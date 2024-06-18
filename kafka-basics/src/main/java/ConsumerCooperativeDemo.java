import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerCooperativeDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCooperativeDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Consumer");

        String groupId = "my-java-application";
        //Create Producer Properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "https://genuine-warthog-10076-us1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Z2VudWluZS13YXJ0aG9nLTEwMDc2JBR5PXMtYb98o5gDxBdm3pm6ldVzetjIg8I\" password=\"YjYwMDQ0MmEtYjU0Zi00MjAyLTk0NjgtZjJiMzI2Nzc4ODJj\";");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "earliest"); //none/earliest/latest
        props.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        //props.put("group.instance.id", ".."); //Strategy for static assignment, give different names for each consumer you span

        //Create Consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            String topic = "demo_java";

            //Get reference to main thread
            final Thread mainThread = Thread.currentThread();

            //Add a shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    log.info("Detected a shutdown, lets exit by calling customer.wakeup()..");
                    consumer.wakeup();
                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            consumer.subscribe(Arrays.asList(topic));
            //Poll for data
            while(true) {
                //log.info("Polling...");
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                consumerRecords.forEach(record -> {
                    log.info("Key: {} Value: {}", record.key(), record.value());
                    log.info("Partition: {} Offset: {}", record.partition(), record.offset());
                });
            }
        } catch (WakeupException we) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.error("Unexpected exception occurred, e");
        }
    }
}
