import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.rabbitmq.client.*;
import connection.BrokerConnection;
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

    private void sendOnce() throws IOException, TimeoutException {
        Channel channel = BrokerConnection.getInst().getChannel(queueName);
        if (channel!=null && channel.isOpen()) {
            channel.basicPublish("", queueName, null, message.getBytes());
            logger.info(String.format("Producer: %s send: %s", this.id,message));
        }
        else {
            logger.error(String.format("channel not ready: %s", this.queueName));
        }
    }

    private void sendEndless() throws IOException, InterruptedException, TimeoutException {
        int count = 0;
        while (true) {
            try {
                count++;
                String msg = "(Run " + count + ") " + message;
                Channel channel = BrokerConnection.getInst().getChannel(queueName);
                if (null != channel && channel.isOpen()) {
                    channel.basicPublish("", queueName, null, msg.getBytes());
                }
                logger.info(String.format("Producer: %s send: %s", this.id,msg));
                if (sleep!=null) {
                    Thread.sleep(sleep);
                }
            }
            catch(Exception e) {
                logger.error(String.format("[%s] %s", e.getClass().getName(),e.getMessage()));
            }
        }
    }

    private void run() throws IOException, TimeoutException {
        try {
            logger.info(String.format("Hi, I am Producer with id: %s", this.id));
            BrokerConnection.getInst().init(address,port);
            if (deamon) {
                sendEndless();
            }
            else {
                sendOnce();
            }
        }
        catch (Exception e) {
            logger.error(String.format("[%s] %s", e.getClass().getName(),e.getMessage()));
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
