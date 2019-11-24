package ca.anandjoshi.kafka;

import com.twitter.hbc.core.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterReader {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(TwitterReader.class);
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        System.out.println("Reading twitter stream");

        List<String> terms = Arrays.asList(new String[] {"kafka", "kubecon", "docker", "java", "python"});

        Client twitterClient = TwitterClient.createTwitterClient(terms, msgQueue);
        twitterClient.connect();

        TwitterKafkaProducer producer = new TwitterKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            twitterClient.stop();
            logger.info("closing producer...");
            producer.shutdownProducer();
            logger.info("done!");
        }));

        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }
            if (msg != null){
                logger.info(msg);
                producer.writeToKafka("twitter-stream", null, msg);
            }
        }
        logger.info("End of application");
    }
}
