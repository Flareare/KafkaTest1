package org.example;

public clas s KafkaDemo {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "demo-group";
    private static final int MESSAGE_COUNT = 5;

    public static void main(String[] args) throws Exception {
        System.out.println("=== Демка Apache Kafka ===");
        System.out.println("Bootstrap-сервер: " + BOOTSTRAP_SERVERS + ", топик: " + MessageSender.TOPIC);

        MessageReceiver receiver = new MessageReceiver(BOOTSTRAP_SERVERS, GROUP_ID);
        Thread consumerThread = new Thread(receiver, "kafka-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();

        System.out.println("Ожидание назначения партиций консьюмеру...");
        receiver.awaitReady();

        try (MessageSender sender = new MessageSender(BOOTSTRAP_SERVERS)) {
            for (int i = 1; i <= MESSAGE_COUNT; i++) {
                sender.send(i);
                Thread.sleep(200);
            }
            sender.flush();
        }

        Thread.sleep(1500);

        receiver.stop();
        consumerThread.join(3000);

        System.out.println("=== Демка завершена ===");
    }
}
