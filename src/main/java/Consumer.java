import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Consumer {
    private final static Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

    @Parameter(names = {"-a", "--address"}, description = "Address of message broker")
    private String address;

    @Parameter(names = {"-p", "--port"}, description = "Port on that message broker listens")
    private Integer port;

    @Parameter(names = {"-i", "--id"}, description = "ID to identify instance", required = true)
    private String id;

    @Parameter(names = {"-q", "--queue"}, description = "Queue to send stuff in")
    private String queueName;

    @Parameter(names = {"-d", "--deamon"}, description = "runs producer as deamon")
    private boolean deamon = false;

    private ConnectionFactory factory;
    private Connection connection = null;
    private Channel channel = null;


    private void receiveOnce(com.rabbitmq.client.Consumer consumer) throws IOException {
        channel.basicConsume(this.queueName, true, consumer);
    }

    private void receiveEndless(com.rabbitmq.client.Consumer consumer) throws IOException {
        while (true) {
            try {
                if (!connection.isOpen()) {
                    connect();
                }
                if (connection.isOpen()) {
                    channel.basicConsume(this.queueName, true, consumer);
                } else {
                    logger.info("Consumer-" + id + " connection closed >:(");
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void run() throws IOException, TimeoutException {
        try {
            logger.info("Hi, I am Consumer with id: " + id);
            factory = new ConnectionFactory();
            factory.setHost(address);
            factory.setPort(port);
            factory.setAutomaticRecoveryEnabled(true);
            connect();

            com.rabbitmq.client.Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String message = new String(body, "UTF-8");
                    logger.info("Consumer-" + id + " receive: " + message);
                }
            };
            if (deamon) {
                receiveEndless(consumer);
            } else {
                receiveOnce(consumer);
            }
        } catch (Exception e) {
            logger.error("Consumer-" + this.id + ": [" + e.getClass().getName() + "] " + e.getMessage());
        } finally {
            if (channel != null) {
                channel.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Consumer consumer = new Consumer();
        JCommander.newBuilder()
                .addObject(consumer)
                .build()
                .parse(args);

        if (consumer.queueName == null) {
            consumer.queueName = Producer.DEFAULT_QUEUE;
        }
        if (consumer.address == null) {
            consumer.address = Producer.DEFAULT_ADDRESS;
        }
        if (consumer.port == null) {
            consumer.port = Producer.DEFAULT_PORT;
        }

        consumer.run();
    }

    private void connect() throws IOException, TimeoutException {
        try {
            if (channel != null) {
                channel.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            logger.error(e.getClass().getName() + ": " + e.getMessage());
        }
        factory = new ConnectionFactory();
        factory.setHost(address);
        factory.setPort(port);
        factory.setAutomaticRecoveryEnabled(false);
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(queueName, false, false, false, null);
    }
}
