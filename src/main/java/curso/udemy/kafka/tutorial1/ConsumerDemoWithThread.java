package curso.udemy.kafka.tutorial1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {

    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "consumer-demo-thread";
        String topic = "first_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Create the consumer thread");
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }
        ));

        try {
            latch.await();

        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }
}
