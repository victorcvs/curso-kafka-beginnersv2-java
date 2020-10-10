package curso.udemy.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable {
    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;
    private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

    public ConsumerRunnable(String bootstrapServers,
                            String groupId,
                            String topic,
                            CountDownLatch latch) {
        this.latch = latch;

        //Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");    //earliest - from de very beginning of the topic
                                                                                        // latest - only new messages on words
        //Create consumer
        consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe consumer to topic(s)
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        //poll for new data
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100l));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: " + record.key() + "\n" +
                            "Value: " + record.value() + "\n" +
                            "Partition: " + record.partition() + " ," +
                            "Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            logger.info("Received shutdown signal!");
        } finally {
            consumer.close();
            //tell our main code we're done with the consume
            latch.countDown();
        }

    }

    public void shutdown() {
        // consumer.wakeup() is a special method to interrupt consumer.poll()
        // it will throw the exception WakeupException
        consumer.wakeup();

    }

}
