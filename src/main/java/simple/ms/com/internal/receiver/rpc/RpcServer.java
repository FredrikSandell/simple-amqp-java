package simple.ms.com.internal.receiver.rpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import simple.ms.com.internal.AmqpConnectionManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

public class RpcServer {

    private final QueueListener queueListener;
    private final Future<?> multiplexerThreadFuture;

    public RpcServer(ExecutorService executorService,
                     AmqpConnectionManager amqpConnectionManager,
                     String endpoint,
                     Function<String, String> function,
                     long globalTtl) {
        this.queueListener = new QueueListener(
                executorService,
                amqpConnectionManager,
                endpoint,
                function,
                globalTtl);
        multiplexerThreadFuture = executorService.submit(queueListener);
    }

    public void stop() {
        this.queueListener.shouldRun = false;
        multiplexerThreadFuture.cancel(true);
    }

    private static class QueueListener implements Runnable {

        private final AmqpConnectionManager amqpConnectionManager;
        private final String endpoint;
        private final long globalTtl;
        private final Function<String, String> function;
        private volatile boolean shouldRun = true;
        private ExecutorService executorService;

        public QueueListener(
                ExecutorService executorService,
                AmqpConnectionManager amqpConnectionManager,
                String endpoint,
                Function function,
                long globalTtl) {
            this.amqpConnectionManager = amqpConnectionManager;
            this.endpoint = endpoint;
            this.globalTtl = globalTtl;
            this.function = function;
            this.executorService = executorService;
        }

        @Override
        public void run() {
            try {
                Channel channel = amqpConnectionManager.getConnection().createChannel();
                Map<String, Object> args = new HashMap<String, Object>();
                args.put("x-message-ttl", globalTtl);

                QueueingConsumer consumer = new QueueingConsumer(channel);
                // The queue name is calculated based on the endpoint string.
                // This means that if several rpc servers use the same endpoint they will share a single queue.
                // Messages received on the queue will be load balanced between RpcMessageServers.
                String queueName = channel.queueDeclare(
                        endpoint+"_rpcQueue",
                        false,  //durable = false, messages will not be persisted
                        false, //exclusive = false, several consumers may connect to this queue
                        true, //auto delete = true, queue will be deleted once the client disconnects
                        args).getQueue();
                channel.basicQos(1);
                channel.basicConsume(queueName, false, consumer);
                final String exchangeName = endpoint;
                channel.exchangeDeclare(
                        exchangeName,
                        "fanout", // messages sent to this exchange will be delivered to all queues.
                        //in practice this will not happen since the queue name is a function of the endpoint
                        // given to the RpcMessageServer. Several RpcMessageServer's with the same endpoint
                        // will therefore share a single queue
                        false, //messages will not be persisted
                        true, //remove the exchange when no queues are connected
                        args);
                channel.queueBind(queueName, exchangeName, ""); //ignore routing keys. Listen to everything
                while(this.shouldRun) {
                    QueueingConsumer.Delivery delivery = consumer.nextDelivery(1000);
                    if(delivery != null) {
                        this.executorService.submit(() -> {
                                    AMQP.BasicProperties props = delivery.getProperties();
                                    try {
                                        String message = new String(delivery.getBody(), "UTF-8");
                                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                                        String response = this.function.apply(message);

                                        AMQP.BasicProperties replyProperties = new AMQP.BasicProperties
                                                .Builder()
                                                .expiration(this.globalTtl+"")
                                                .contentEncoding("UTF-8")
                                                .correlationId(props.getCorrelationId())
                                                .deliveryMode(1)
                                                .build();

                                        channel.basicPublish("", props.getReplyTo(), replyProperties, response.getBytes("UTF-8"));
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                        );
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                return;
            }
        }
    }
}