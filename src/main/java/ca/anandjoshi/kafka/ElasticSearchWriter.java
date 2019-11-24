package ca.anandjoshi.kafka;

import com.twitter.hbc.core.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ElasticSearchWriter {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ElasticSearchWriter.class);

        System.out.println("Writing twitter stream to Elastic Search");

        TwitterKafkaConsumer consumer = new TwitterKafkaConsumer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("closing producer...");
            consumer.shutdownConsumer();
            logger.info("done!");
        }));

        consumer.readFromKafka("twitter-stream");
        logger.info("End of application");
    }
}
