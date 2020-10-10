package curso.udemy.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";

        //Create the producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer record
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties); // Chave String, mensagem(value) String

        //Create a producer record
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "novo teste java");

        //Send data - assincrono
        producer.send(record);

        producer.flush();  // envia as mensagens

        producer.close(); //envia as mensagens e encerra o producer

    }
}
