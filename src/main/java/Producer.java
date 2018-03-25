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

    @Parameter(names = { "-d", "--deamon" }, description = "runs producer as deamon")
    private boolean deamon = false;

    @Parameter(names = { "-s", "--sleep" }, description = "sleep time between send request")
    private Integer sleep;

    private ConnectionFactory factory;
    private Connection connection = null;
    private Channel channel = null;

    private void sendOnce() throws IOException {
        channel.basicPublish("", queueName, null, message.getBytes());
        logger.error ("Producer-" + this.id + " send: " + message);
    }

    private void sendEndless() throws IOException, InterruptedException {
        int count = 0;
        while (true) {
            try {
                if (!connection.isOpen()) {
                    connect();
                }
                count++;
                String msg = "(Run " + count + ") " + message;
                if (connection.isOpen()) {
                    channel.basicPublish("", queueName, null, msg.getBytes());
                    logger.info("Producer-" + this.id + " send: " + msg);
                }
                else {
                    logger.info("Producer-" + this.id + " connection closed >:(");
                }
                if (sleep!=null) {
                    Thread.sleep(sleep);
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void connect() throws IOException, TimeoutException {
        try {
            if (channel!=null) {
                channel.close();
            }
            if (connection!=null) {
                connection.close();
            }
        }
        catch(Exception e) {
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

    private void run() throws IOException, TimeoutException {
        try {
            logger.info("Hi, I am Producer with id: "+id);
            connect();
            if (deamon) {
                sendEndless();
            }
            else {
                sendOnce();
            }
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
