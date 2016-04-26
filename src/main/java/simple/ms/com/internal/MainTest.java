package simple.ms.com.internal;

/**
 * Created by liket on 2015-11-23.
 */
public class MainTest {

    public static void main(String[] args) throws InterruptedException {
        EndpointFactory endpointFactory = new EndpointFactory.Builder().setHost("localhost").build();
/*
        endpointFactory.getBroadcastEventReceiver("/event/endpoint1", event -> {
            System.out.println("Receiver 1: " + event);
        });
        endpointFactory.getBroadcastEventReceiver("/event/endpoint1", event -> {
            System.out.println("Receiver 2: " + event);
        });
        Thread.sleep(3000);


        System.out.println("Sent event: " + endpointFactory.getEventEmitter().emit("/event/endpoint1", "hello event1"));

        Thread.sleep(5000);
*/

        endpointFactory.getRpcServer("/rpc/endpoint2", msg -> {
            System.out.println("Handling: " + msg);
            return "handled: " + msg;
        });

        Thread.sleep(4000);

        endpointFactory.getRpcClient().call("/rpc/endpoint2", "request message1", 2000).thenAccept(returnValue -> {
            System.out.println("The result is: " + returnValue);
        });

        Thread.sleep(4000);
        //broadcastEventReceiver1.stop();
        //endpointFactory.terminate();
    }
}
