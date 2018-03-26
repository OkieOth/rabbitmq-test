package connection;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * This class encapsulate the connection to the message broker. It is prepared to handle
 * multiple channel
 */
public class BrokerConnection {
    private final static Logger logger = LoggerFactory.getLogger(BrokerConnection.class.getName());
    static BrokerConnection inst = null;

    private ConnectionFactory factory;
    private Connection connection = null;
    private Map<String, Channel> channelMap = null;

    private String address;
    private int port;
    private int networkRecoveryInterval = 10000;
    private int requestedHeartbeat = 10;
    private int connectionTimeout = 5000;

    private BrokerConnection() {
        channelMap = new HashMap<>();
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getNetworkRecoveryInterval() {
        return networkRecoveryInterval;
    }

    public void setNetworkRecoveryInterval(int networkRecoveryInterval) {
        this.networkRecoveryInterval = networkRecoveryInterval;
        if (this.factory != null) {
            factory.setNetworkRecoveryInterval(this.networkRecoveryInterval);
        }
    }

    public int getRequestedHeartbeat() {
        return requestedHeartbeat;
    }

    public void setRequestedHeartbeat(int requestedHeartbeat) {
        this.requestedHeartbeat = requestedHeartbeat;
        if (this.factory != null) {
            factory.setRequestedHeartbeat(this.requestedHeartbeat);
        }
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        if (this.factory != null) {
            factory.setConnectionTimeout(this.connectionTimeout);
        }
    }

    public void init(String address, int port) {
        this.address = address;
        this.port = port;
        factory = new ConnectionFactory();
        factory.setHost(this.address);
        factory.setPort(this.port);
        factory.setNetworkRecoveryInterval(this.networkRecoveryInterval);
        factory.setRequestedHeartbeat(this.requestedHeartbeat);
        factory.setConnectionTimeout(this.connectionTimeout);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(false);
        if (logger.isInfoEnabled()) {
            logger.info ("init broker connection: address="+this.address+", port="+this.port);
        }
    }

    private void initConnection() throws IOException, TimeoutException {
        connection = factory.newConnection();
        this.connection.addShutdownListener(new ShutdownListener() {
            @Override
            public void shutdownCompleted(ShutdownSignalException arg0) {
                logger.error("connection shutdown");
            }
        });
        this.connection.addBlockedListener(new BlockedListener() {
            @Override
            public void handleBlocked(String reason) throws IOException {
                logger.error("connection blocked: " + reason);
            }

            @Override
            public void handleUnblocked() throws IOException {
                logger.info("block released");
            }
        });
    }

    private Channel initChannel(String channelName) throws IOException, TimeoutException {
        if (connection == null || (!connection.isOpen())) {
            initConnection();
        }
        Channel channel = connection.createChannel();
        channel.queueDeclare(channelName, false, false, false, null);
        channelMap.put(channelName, channel);
        return channel;
    }

    public synchronized Channel getChannel(String channelName) throws IOException, TimeoutException {
        Channel channel = this.channelMap.get(channelName);
        if (channel == null || (!channel.isOpen())) {
            return initChannel(channelName);
        } else {
            return channel;
        }
    }

    public synchronized void freeChannel(String channelName) throws IOException, TimeoutException {
        Channel channel = this.channelMap.get(channelName);
        if (channel == null) {
            return;
        }
        this.channelMap.remove(channelName);
        try {
            channel.close();
        }
        catch (Exception e) {
            logger.error(String.format("[%s] %s", e.getClass().getName(),e.getMessage()));
        }
    }

    public static BrokerConnection getInst() {
        if (inst == null) {
            inst = new BrokerConnection();
        }
        return inst;
    }
}
