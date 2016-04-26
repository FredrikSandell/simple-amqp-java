package simple.ms.com.internal.sender.rpc;

/**
 * Created by liket on 2015-12-22.
 */
public class ServerException extends RuntimeException {

    private final String serverMessage;

    public ServerException(String serverMessage) {
        this.serverMessage = serverMessage;
    }

    public String getServerMessage() {
        return serverMessage;
    }
}
