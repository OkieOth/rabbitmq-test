import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.rabbitmq.client.*;
import connection.BrokerConnection;
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

    private com.rabbitmq.client.Consumer createConsumer(Channel channel) {
        return new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                logger.info(String.format("Consumer: %s receive: %s", id,message));
            }
        };
    }

    private void receiveOnce() throws IOException, TimeoutException {
        Channel channel = BrokerConnection.getInst().getChannel(queueName);
        if (channel!=null && channel.isOpen()) {
            channel.basicConsume(this.queueName, true, createConsumer(channel));
        }
        else {
            logger.error(String.format("channel not ready: %s", this.queueName));
        }
    }

    private void receiveEndless() {
        com.rabbitmq.client.Consumer consumer = null;
        Channel channel = null;
        while (true) {
            try {
                if (channel!=null && channel.isOpen()) {
                    if (consumer==null) consumer = createConsumer(channel);
                    channel.basicConsume(this.queueName, true,consumer);
                }
                else {
                    channel = BrokerConnection.getInst().getChannel(queueName);
                    logger.error(String.format("channel not ready: %s", this.queueName));
                }
            } catch (Exception e) {
                logger.error(String.format("[%s] %s", e.getClass().getName(),e.getMessage()));
                try {
                    if (channel!=null) channel.close();
                }
                catch(Exception e2) {
                    logger.error(String.format("error while additional channel close: [%s] %s", e.getClass().getName(),e.getMessage()));
                }
                channel = null;
                consumer = null;
            }
        }
    }

    private void run() throws IOException, TimeoutException {
        try {
            logger.info("Hi, I am Consumer with id: " + id);
            BrokerConnection.getInst().init(address,port);

            if (deamon) {
                receiveEndless();
            } else {
                receiveOnce();
            }
        } catch (Exception e) {
            logger.error("Consumer-" + this.id + ": [" + e.getClass().getName() + "] " + e.getMessage());
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
}
