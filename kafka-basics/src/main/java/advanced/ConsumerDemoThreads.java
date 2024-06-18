package advanced;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThreads {

    public static void main(String[] args)  {
        ConsumerDemoWorker consumerDemoWorker = new ConsumerDemoWorker();
        new Thread(consumerDemoWorker).start();
        Runtime.getRuntime().addShutdownHook(new Thread(new ConsumerDemoCloser(consumerDemoWorker)));
    }

    private static class ConsumerDemoWorker implements Runnable {

        private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWorker.class);

        private CountDownLatch countDownLatch;
        private Consumer<String, String> consumer;

        @Override
        public void run() {
            countDownLatch = new CountDownLatch(1);
            final Properties properties = new Properties();

            Properties props = new Properties();
            props.put("bootstrap.servers", "https://genuine-warthog-10076-us1-kafka.upstash.io:9092");
            props.put("sasl.mechanism", "SCRAM-SHA-256");
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Z2VudWluZS13YXJ0aG9nLTEwMDc2JBR5PXMtYb98o5gDxBdm3pm6ldVzetjIg8I\" password=\"YjYwMDQ0MmEtYjU0Zi00MjAyLTk0NjgtZjJiMzI2Nzc4ODJj\";");
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());
            //props.put("group.id", groupId);
            props.put("auto.offset.reset", "earliest"); //none/earliest/latest

            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singleton("demo_java"));

            final Duration pollTimeout = Duration.ofMillis(100);

            try {
                while (true) {
                    final ConsumerRecords<String, String> consumerRecords = consumer.poll(pollTimeout);
                    for (final ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        log.info("Getting consumer record key: '" + consumerRecord.key() + "', value: '" + consumerRecord.value() + "', partition: " + consumerRecord.partition() + " and offset: " + consumerRecord.offset() + " at " + new Date(consumerRecord.timestamp()));
                    }
                }
            } catch (WakeupException e) {
                log.info("Consumer poll woke up");
            } finally {
                consumer.close();
                countDownLatch.countDown();
            }
        }

        void shutdown() throws InterruptedException {
            consumer.wakeup();
            countDownLatch.await();
            log.info("Consumer closed");
        }

    }

    private static class ConsumerDemoCloser implements Runnable {

        private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCloser.class);

        private final ConsumerDemoWorker consumerDemoWorker;

        ConsumerDemoCloser(final ConsumerDemoWorker consumerDemoWorker) {
            this.consumerDemoWorker = consumerDemoWorker;
        }

        @Override
        public void run() {
            try {
                consumerDemoWorker.shutdown();
            } catch (InterruptedException e) {
                log.error("Error shutting down consumer", e);
            }
        }
    }
}