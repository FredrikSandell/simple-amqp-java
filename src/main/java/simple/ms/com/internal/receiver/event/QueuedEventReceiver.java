package simple.ms.com.internal.receiver.event;

import simple.ms.com.internal.AmqpConnectionManager;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Created by liket on 2016-01-13.
 */
public class QueuedEventReceiver extends EventReceiver {
    public QueuedEventReceiver(ExecutorService executorService, AmqpConnectionManager amqpConnectionManager, String endpoint, long globalTtl, Consumer<String> consumer) {
        super(executorService, amqpConnectionManager, endpoint, globalTtl, consumer, false);
    }
}
