package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageReceiver implements Runnable, AutoCloseable {

    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch partitionsAssigned = new CountDownLatch(1);

    public MessageReceiver(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(MessageSender.TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                partitionsAssigned.countDown();
            }
        });
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("получено " + record.value()
                            + " [partition=" + record.partition() + ", offset=" + record.offset() + "]");
                }
            }
        } catch (WakeupException e) {
            System.out.println("Consumer остановлен.");
        } finally {
            consumer.close();
        }
    }

    public void awaitReady() throws InterruptedException {
        partitionsAssigned.await();
        System.out.println("Consumer готов, топик назначен.");
    }

    public void stop() {
        running.set(false);
        consumer.wakeup();
    }

    @Override
    public void close() {
        stop();
    }
}
