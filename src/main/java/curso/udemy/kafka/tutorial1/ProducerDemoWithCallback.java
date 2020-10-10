package curso.udemy.kafka.tutorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

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

            // Create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic",
                                          "teste java - with callback" + Integer.toString(i));
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
            });
        }

        producer.flush();  // envia as mensagens

        producer.close(); //envia as mensagens e encerra o producer

    }
}
