package ca.anandjoshi.kafka;

import ca.anandjoshi.utils.TwitterKafkaConsumer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchWriter {

    final private static JsonParser jsonParser = new JsonParser();
    final Logger logger = LoggerFactory.getLogger(ElasticSearchWriter.class);

    public static void main(String[] args) {
        new ElasticSearchWriter().run();
    }

    private void run() {
        System.out.println("Writing twitter stream to Elastic Search");
        TwitterKafkaConsumer consumer = new TwitterKafkaConsumer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("closing producer...");
            consumer.shutdownConsumer();
            logger.info("done!");
        }));

        // TODO: write retrieved records to elastic search
        while (true) {
            ConsumerRecords<String, String> records = consumer.readFromKafka("twitter-stream");
            for (ConsumerRecord<String, String> record : records) {
                try {
                    String id = extractIdFromTweet(record.value());
                    String text = extractTextFromTweet(record.value());
                    logger.info("Received id " + id + " with text " + text + "from twitter");
                } catch (NullPointerException e) {
                    logger.warn("skipping bad data: " + record.value());
                }
            }
            consumer.commitSync();
        }
        //logger.info("End of application");
    }

    private String extractTextFromTweet(String tweet) {
        JsonElement tweetJson = extractJsonFromTweet(tweet);
        return tweetJson.getAsJsonObject()
                .get("text")
                .getAsString();
    }

    private String extractIdFromTweet(String tweet) {
        JsonElement tweetJson = extractJsonFromTweet(tweet);
        return tweetJson.getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    private JsonElement extractJsonFromTweet(String tweet) {
        return jsonParser.parse(tweet);
    }
}
