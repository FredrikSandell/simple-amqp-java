package simple.ms.com.internal;

import simple.ms.com.internal.receiver.event.QueuedEventReceiver;
import simple.ms.com.internal.receiver.event.BroadcastEventReceiver;
import simple.ms.com.internal.receiver.rpc.RpcServer;
import simple.ms.com.internal.sender.event.EventEmitter;
import simple.ms.com.internal.sender.rpc.RpcClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by liket on 2015-11-23.
 */
public class EndpointFactory {
    private final ExecutorService executorService;
    private final long globalTtl;
    private final AmqpConnectionManager amqpConnectionManager;

    EndpointFactory(ExecutorService executorService,
                    long globalTtl,
                    AmqpConnectionManager amqpConnectionManager) {
        this.globalTtl = globalTtl;
        this.amqpConnectionManager = amqpConnectionManager;
        this.executorService = executorService;
    }

    public QueuedEventReceiver getQueuedEventReceiver(String endpoint, Consumer<String> consumer) {
        return new QueuedEventReceiver(
                executorService,
                amqpConnectionManager,
                endpoint,
                globalTtl,
                consumer);
    }

    public BroadcastEventReceiver getBroadcastEventReceiver(String endpoint, Consumer<String> consumer) {
        return new BroadcastEventReceiver(
                executorService,
                amqpConnectionManager,
                endpoint,
                globalTtl,
                consumer);
    }

    public RpcServer getRpcServer(String endpoint, Function<String, String> function) {
        return new RpcServer(
                executorService,
                amqpConnectionManager,
                endpoint,
                function,
                globalTtl);
    }

    public void close() {
        amqpConnectionManager.close();
    }

    public void terminate() {
        amqpConnectionManager.close();
        executorService.shutdown();
    }

    public EventEmitter getEventEmitter() {
        return new EventEmitter(amqpConnectionManager, globalTtl);
    }

    public RpcClient getRpcClient() {
        return new RpcClient(amqpConnectionManager, executorService, globalTtl);
    }

    static public class Builder {
        private ExecutorService executorService;
        private long globalTtl;
        private String host;

        public Builder setExecutorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public Builder setGlobalTtl(long globalTtl) {
            this.globalTtl = globalTtl;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public EndpointFactory build() {
            if (executorService == null) {
                executorService = Executors.newCachedThreadPool();//ForkJoinPool.commonPool();
            }
            if (globalTtl == 0) {
                globalTtl = 60000; //one minute
            }
            if (host == null) {
                throw new IllegalArgumentException("The host parameter needs to be set in order to create a "
                        + EndpointFactory.class.getCanonicalName() + " instance");
            }
            return new EndpointFactory(executorService, globalTtl, new AmqpConnectionManager(host));
        }
    }
}
