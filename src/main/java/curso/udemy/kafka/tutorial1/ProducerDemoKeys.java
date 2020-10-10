package curso.udemy.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        // Create the producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer record
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties); // Chave String,
        // mensagem(value)
        // String
        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "teste java + with key " + i;
            String key = "id_" + i;

            // Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key);

            // Send data - assincrono
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetada, Exception e) {
                    // executa toda vez que uma mensagem foi enviado com sucesso ou em caso de exceção
                    if (e == null) {
                        //Mensagem enviada com sucesso
                        logger.info("Receveid new metadata. \n" +
                                "Topic: " + recordMetada.topic() + "\n" +
                                "Partition: " + recordMetada.partition() + "\n" +
                                "Offset: " + recordMetada.offset() + "\n" +
                                "Timestamp: " + recordMetada.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //block the send() to make it synchronous (bad practice)
        }

        producer.flush();  // envia as mensagens

        producer.close(); //envia as mensagens e encerra o producer

    }
}
