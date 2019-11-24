package ca.anandjoshi.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TwitterKafkaConsumer {

    final private Logger logger = LoggerFactory.getLogger(TwitterKafkaConsumer.class);
    private KafkaConsumer consumer = null;

    private static KafkaConsumer createTwitterKafkaConsumer() {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // disable auto commit of offsets

        // create consumer
        return new KafkaConsumer<>(properties);
    }

    public ConsumerRecords<String, String> readFromKafka(String topic) {
        if (this.consumer == null) {
            consumer = createTwitterKafkaConsumer();
        }
        consumer.subscribe(Arrays.asList(topic));
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            int recordCount = records.count();
            if (recordCount > 0) {
                logger.info("Received " + recordCount + " records");
                return records;
            }
        }
    }

    public void commitSync() {
        logger.debug("Committing offsets...");
        this.consumer.commitSync();
        logger.debug("Offsets have been committed");
    }

    public void shutdownConsumer() {
        logger.debug("closing consumer...");
        this.consumer.close();
    }
}
