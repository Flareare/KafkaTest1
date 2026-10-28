package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MessageSender implements AutoCloseable {

    public static final String TOPIC = "test-topic";

    private final KafkaProducer<String, String> producer;

    public MessageSender(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        this.producer = new KafkaProducer<>(props);
    }

    public void send(int messageNumber) {
        String message = "сообщение " + messageNumber;
        ProducerRecord<String, String> record =
                new ProducerRecord<>(TOPIC, String.valueOf(messageNumber), message);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Ошибка отправки \"" + message + "\": " + exception.getMessage());
            } else {
                System.out.println("отправлено " + message
                        + " [partition=" + metadata.partition() + ", offset=" + metadata.offset() + "]");
            }
        });
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.close();
    }
}
