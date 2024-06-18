import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Producer");

        //Create Producer Properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "https://genuine-warthog-10076-us1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Z2VudWluZS13YXJ0aG9nLTEwMDc2JBR5PXMtYb98o5gDxBdm3pm6ldVzetjIg8I\" password=\"YjYwMDQ0MmEtYjU0Zi00MjAyLTk0NjgtZjJiMzI2Nzc4ODJj\";");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("batch.size", "400");
        //props.put("partitioner.class", RoundRobinPartitioner.class.getName());

        //Create the Producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props)) {
            ProducerRecord<String, String> record;
            String value;
            for(int o = 0; o< 10; o++) {
                for(int i = 0; i< 10; i++) {
                    value = "Kafka with call back " +i;
                    record = new ProducerRecord<>("demo_java", value);
                    //Send Data - asynchronous
                    producer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e == null) {
                                log.info("Received new metadata \n" +
                                        "Topic: " + recordMetadata.topic() + "\n" +
                                        "Partition: " + recordMetadata.partition() + "\n" +
                                        "Offsets: " + recordMetadata.offset() + "\n" +
                                        "TimeStamp: " + recordMetadata.timestamp() + "\n"
                                );
                            }else {
                                log.error("Error while producing", e);
                            }
                        }
                    });
                }
                Thread.sleep(500);
            }

            //Flush - tell the producer to send all data & block until done - synchronous
            producer.flush();
            // Close the Producer
            producer.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
