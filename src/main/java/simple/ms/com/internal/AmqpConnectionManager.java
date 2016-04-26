package simple.ms.com.internal;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by liket on 2015-11-20.
 */
public class AmqpConnectionManager {

    private final String host;
    private Connection connection;

    public AmqpConnectionManager(String host) {
        this.host = host;
    }

    public synchronized Connection getConnection() {
        if(connection == null) {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            try {
                connection = factory.newConnection();
            } catch (IOException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        return connection;
    }

    public void close() {
        try {
            if(connection != null && connection.isOpen()) {
                connection.close(1000);
            }
        } catch (IOException e) {
            return;
        }
    }
}
