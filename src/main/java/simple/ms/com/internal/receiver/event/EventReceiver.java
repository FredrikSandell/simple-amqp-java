package simple.ms.com.internal.receiver.event;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import simple.ms.com.internal.AmqpConnectionManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

abstract class EventReceiver {

    private final QueueListener queueListener;
    private final Future<?> multiplexerThreadFuture;

    public EventReceiver(ExecutorService executorService,
                         AmqpConnectionManager amqpConnectionManager,
                         String endpoint,
                         long globalTtl,
                         Consumer<String> consumer,
                         boolean isBroadcast) {
        this.queueListener = new QueueListener(
                executorService,
                amqpConnectionManager,
                endpoint,
                globalTtl,
                consumer,
                isBroadcast
        );
        multiplexerThreadFuture = executorService.submit(queueListener);
    }

    public void stop() {
        this.queueListener.shouldRun = false;
        this.multiplexerThreadFuture.cancel(true);
    }

    private static class QueueListener implements Runnable {

        private final AmqpConnectionManager amqpConnectionManager;
        private final String endpoint;
        private final long globalTtl;
        private volatile boolean shouldRun = true;
        private final Consumer consumer;
        private ExecutorService executorService;
        private final boolean isBroadcast;

        public QueueListener(
                ExecutorService executorService,
                AmqpConnectionManager amqpConnectionManager,
                String endpoint,
                long globalTtl,
                Consumer consumer,
                boolean isBroadcast) {
            this.amqpConnectionManager = amqpConnectionManager;
            this.endpoint = endpoint;
            this.consumer = consumer;
            this.globalTtl = globalTtl;
            this.executorService = executorService;
            this.isBroadcast = isBroadcast;
        }

        @Override
        public void run() {
            try {
                Channel channel = amqpConnectionManager.getConnection().createChannel();
                QueueingConsumer queueingConsumer = new QueueingConsumer(channel);

                Map<String, Object> args = new HashMap<String, Object>();
                args.put("x-message-ttl", globalTtl);

                String queueName = channel.queueDeclare(
                        //if it is a broadcast event all consumers will have their own queues. All receivers will
                        //therefore receive a copy of the event. Otherwise the receiver is considered "queued" and
                        //all receivers will share a single queue. Thus only one receiver will get each event.
                        isBroadcast ? UUID.randomUUID().toString() + "eventQueue" : endpoint,
                        false, //durable = false, messages on this queue will not be persisted to disk
                        isBroadcast, //exclusive = isBroadcast, if it is a broadcast receiver the queue will only be used by this single client
                        true, //autoDelete = true, remove queue when client disconnects
                        args).getQueue();
                channel.basicQos(1);
                channel.basicConsume(queueName, false, queueingConsumer);
                final String exchangeName = endpoint;
                channel.exchangeDeclare(
                        exchangeName,
                        "fanout", //All subscribers to the exchange get at copy of the message
                        false, // durable = false, messages will not be persisted
                        true, // autoDelete = true, remove when client disconnects (when no queues are connected)
                        args);
                //bind newly created queue to listen to anything sent to the exchange
                channel.queueBind(queueName, endpoint, "");

                while (this.shouldRun) {
                    QueueingConsumer.Delivery delivery = queueingConsumer.nextDelivery(1000);
                    if (delivery != null) {
                        this.executorService.submit(() -> {
                            try {
                                String message = new String(delivery.getBody(), "UTF-8");
                                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                                this.consumer.accept(message);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
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