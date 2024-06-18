import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKey {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKey.class.getSimpleName());

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

        //Create the Producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props)) {
            ProducerRecord<String, String> record;
            String topic = "demo_java";
            String value;
            for(int j = 0; j < 2; j++) {
                for(int i = 0; i < 10; i++) {
                    String key = "id_"+i;
                    value = "Kafka with key " +i;
                    record = new ProducerRecord<>(topic, key, value);
                    //Send Data - asynchronous
                    producer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e == null) {
                                log.info("Key: " + key +
                                        " | Partition: " + recordMetadata.partition() /*+ "\n"
                                        "Offsets: " + recordMetadata.offset() + "\n" +
                                        "TimeStamp: " + recordMetadata.timestamp() + "\n"*/
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
