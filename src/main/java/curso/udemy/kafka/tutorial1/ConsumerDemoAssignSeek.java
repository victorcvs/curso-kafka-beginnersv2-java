package curso.udemy.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "first_topic";

        //Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Assign and Seek are the mostly used to replay data or fetch a specific message

        //Assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0); //read partition 0 from our topic
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        long offsetToRead = 15L;
        consumer.seek(partitionToReadFrom, offsetToRead); //read offset 15 from partition

        //poll for new data
        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;
        boolean keepReading = true;
        while (keepReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + "\n" +
                        "Value: " + record.value() + "\n" +
                        "Partition: " + record.partition() + " ," +
                        "Offset: " + record.offset());
                numberOfMessagesReadSoFar++;
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepReading = false; // exit the while loop
                    break; // to exit the for loop
                }
            }
        }

    }
}
