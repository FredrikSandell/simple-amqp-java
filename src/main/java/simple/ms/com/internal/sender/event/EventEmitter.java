package simple.ms.com.internal.sender.event;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownSignalException;
import simple.ms.com.internal.AmqpConnectionManager;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class EventEmitter {

    private final AmqpConnectionManager amqpConnectionManager;
    private final long globalTtl;

    public EventEmitter(AmqpConnectionManager amqpConnectionManager, long globalTtl) {
        this.amqpConnectionManager = amqpConnectionManager;
        this.globalTtl = globalTtl;
    }

    public boolean emit(String endpoint, String message) {
        Connection connection = amqpConnectionManager.getConnection();
        try {
            Channel channel = connection.createChannel();
            BasicProperties props = new BasicProperties
                    .Builder()
                    .contentEncoding("UTF-8")
                    .deliveryMode(1)
                    .expiration(globalTtl + "")
                    .build();
            channel.basicPublish(endpoint, "", props, message.getBytes("UTF-8"));
            channel.close();
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (ShutdownSignalException e) {
            return false;
        }
    }
}