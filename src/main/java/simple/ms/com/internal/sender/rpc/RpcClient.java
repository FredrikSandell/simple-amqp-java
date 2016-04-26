package simple.ms.com.internal.sender.rpc;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import simple.ms.com.internal.AmqpConnectionManager;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

public class RpcClient {

    private final AmqpConnectionManager amqpConnectionManager;
    private final long globalTtl;
    private final ExecutorService responseExecutorService;

    public RpcClient(AmqpConnectionManager amqpConnectionManager,
                     ExecutorService responseExecutorService,
                     long globalTtl) {
        this.amqpConnectionManager = amqpConnectionManager;
        this.responseExecutorService = responseExecutorService;
        this.globalTtl = globalTtl;
    }

    public CompletableFuture<String> call(String endpoint, String requestMessage, long timeout) {
        if(timeout > this.globalTtl) {
            throw new IllegalArgumentException("The timeout must be lower or equal to the predefined global timeout. (Currently set to "+this.globalTtl+")");
        }
        try {
            Channel channel = amqpConnectionManager.getConnection().createChannel();
            Map<String, Object> args = new HashMap<String, Object>();
            args.put("x-message-ttl", RpcClient.this.globalTtl);

            QueueingConsumer consumer = new QueueingConsumer(channel);
            String replyQueueName = channel.queueDeclare(
                    UUID.randomUUID().toString()+"_rpcReplyQueue",
                    false,  //durable = false, messages will not be persisted
                    true, //exclusive = true, several consumers may not connect to this queue
                    true, //auto delete = true, queue will be deleted once the client disconnects
                    args).getQueue();
            channel.basicQos(1);
            channel.basicConsume(replyQueueName, false, consumer);

            CompletableFuture<String> completableResponse = CompletableFuture.supplyAsync(() -> {
                try {

                    channel.basicPublish(endpoint, "", new BasicProperties.Builder()
                            .expiration(timeout + "")
                            .replyTo(replyQueueName)
                            .contentEncoding("UTF-8")
                            .deliveryMode(1)
                            .build()
                            , requestMessage.getBytes("UTF-8"));
                    //block until message received or timeout reached
                    QueueingConsumer.Delivery delivery = consumer.nextDelivery(timeout);
                    //The queue will be deleted anyway once we disconnect from it. But better safe than sorry
                    channel.queueDelete(replyQueueName);
                    if (delivery != null) {
                        String replyMessage = new String(delivery.getBody(), "UTF-8");
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        return replyMessage;
                    } else {
                        throw new ReplyTimeoutException();
                    }
                //These error are likely non recoverable.
                } catch (UnsupportedEncodingException e) {
                    throw new IllegalArgumentException(e);
                } catch (IOException | InterruptedException e) {
                    throw new IllegalStateException(e);
                } finally {
                    try {
                        channel.close();
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    } catch (TimeoutException e) {
                        throw new IllegalStateException(e);
                    }
                }
            }, this.responseExecutorService);
            return completableResponse;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}