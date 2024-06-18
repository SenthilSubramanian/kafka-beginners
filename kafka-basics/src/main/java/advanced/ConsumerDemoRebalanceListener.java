package advanced;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoRebalanceListener {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoRebalanceListener.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer with a Rebalance");

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fifth-application";
        String topic = "demo_java";

        // create consumer configs
        Properties props = new Properties();
        props.put("bootstrap.servers", "https://genuine-warthog-10076-us1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Z2VudWluZS13YXJ0aG9nLTEwMDc2JBR5PXMtYb98o5gDxBdm3pm6ldVzetjIg8I\" password=\"YjYwMDQ0MmEtYjU0Zi00MjAyLTk0NjgtZjJiMzI2Nzc4ODJj\";");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        //props.put("group.id", groupId);
        props.put("auto.offset.reset", "earliest"); //none/earliest/latest

        // we disable Auto Commit of offsets
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        ConsumerRebalanceListenerImpl listener = new ConsumerRebalanceListenerImpl(consumer);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic), listener);

            // poll for new data
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                    // we track the offset we have been committed in the listener
                    listener.addOffsetToTrack(record.topic(), record.partition(), record.offset());
                }

                // We commitAsync as we have processed all data and we don't want to block until the next .poll() call
                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            log.info("Wake up exception!");
            // we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            try {
                consumer.commitSync(listener.getCurrentOffsets()); // we must commit the offsets synchronously here
            } finally {
                consumer.close();
                log.info("The consumer is now gracefully closed.");
            }
        }
    }
}