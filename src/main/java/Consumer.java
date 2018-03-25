import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Consumer {
    private final static Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

    @Parameter(names = { "-a", "--address" }, description = "Address of message broker")
    private String address;

    @Parameter(names = { "-p", "--port" }, description = "Port on that message broker listens")
    private Integer port;

    @Parameter(names = { "-i", "--id" }, description = "ID to identify instance", required = true)
    private String id;

    @Parameter(names = { "-q", "--queue" }, description = "Queue to send stuff in")
    private String queueName;

    private void run() throws IOException, TimeoutException {
        Connection connection = null;
        Channel channel = null;
        try {
            logger.info("Hi, I am Consumer with id: "+id);
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(address);
            factory.setPort(port);
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare(queueName, false, false, false, null);

            com.rabbitmq.client.Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String message = new String(body, "UTF-8");
                    logger.error ("Consumer-" + id + " receive: " + message);
                }
            };
            while (true) {
                channel.basicConsume(this.queueName, true, consumer);
            }

        }
        catch (Exception e) {
            logger.error ("Consumer-" + this.id + ": [" + e.getClass().getName() + "] " + e.getMessage());
        }
        finally {
            if (channel!=null) {
                channel.close();
            }
            if (connection!=null) {
                connection.close();
            }
        }
    }

    public static void main (String[] args) throws Exception {
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
