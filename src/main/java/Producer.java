import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
    private final static Logger logger = LoggerFactory.getLogger(Producer.class.getName());
    public static final String DEFAULT_QUEUE = "mySpecialQue";
    private static final String DEFAULT_MESSAGE = "Hello ";
    public static final String DEFAULT_ADDRESS = "localhost";
    public static final Integer DEFAULT_PORT = 5672;

    @Parameter(names = { "-a", "--address" }, description = "Address of message broker")
    private String address;

    @Parameter(names = { "-p", "--port" }, description = "Port on that message broker listens"  )
    private Integer port;

    @Parameter(names = { "-i", "--id" }, description = "ID to identify instance", required = true)
    private String id;

    @Parameter(names = { "-q", "--queue" }, description = "Queue to send stuff in")
    private String queueName;

    @Parameter(names = { "-m", "--msg" }, description = "Message to send")
    private String message;

    private void run() throws IOException, TimeoutException {
        Connection connection = null;
        Channel channel = null;
        try {
            logger.info("Hi, I am Producer with id: "+id);
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(address);
            factory.setPort(port);
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare(queueName, false, false, false, null);
            channel.basicPublish("", queueName, null, message.getBytes());
            logger.error ("Producer-" + this.id + " send: " + message);
        }
        catch (Exception e) {
            logger.error ("Producer-" + this.id + ": [" + e.getClass().getName() + "] " + e.getMessage());
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
        Producer producer = new Producer();
        JCommander.newBuilder()
                .addObject(producer)
                .build()
                .parse(args);
        if (producer.queueName == null) {
            producer.queueName = DEFAULT_QUEUE;
        }
        if (producer.message == null) {
            producer.message = DEFAULT_MESSAGE + new Date().toString();
        }
        if (producer.address == null) {
            producer.address = DEFAULT_ADDRESS;
        }
        if (producer.port == null) {
            producer.port = DEFAULT_PORT;
        }

        producer.run();
        logger.info("finished");
    }
}
