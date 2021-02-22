package server;

import com.google.gson.Gson;
import shared.messages.KVAdminMessage;

import java.util.Arrays;

public class ServerMetaData {
    private int cacheSize;
    private String cacheStrategy;
    private int receiveDataPort;
    private String host;
    private ServerDataTransferProgressStatus status;
    private ServerResponseStatus responseStatus;

    public enum ServerDataTransferProgressStatus {
        IN_PROGRESS,       /* If data transfer is in progress */
        IDLE               /* Server status is idle */
    }

    public enum ServerResponseStatus{
        ACK,
        ERROR
    }

    public ServerMetaData(int cacheSize, String cacheStrategy, int port, String host) {
        this.cacheSize = cacheSize;
        this.cacheStrategy = cacheStrategy;
        this.receiveDataPort = port;
        this.host = host;
    }

    @Override
    public String toString() {
        return "ServerMetaData{" +
                "cacheSize='" + cacheSize +
                ", cacheStrategy=" + cacheStrategy  + '\'' +
                ", receiveDataPort='" + receiveDataPort + '\'' +
                ", host='" + host + '\'' +
                ", status=" + status +
                '}';
    }

    public String encode() {
        return new Gson().toJson(this);
    }

    public void decode(String input) {
        ServerMetaData msg = new Gson().fromJson(input, this.getClass());
        this.cacheSize = msg.cacheSize;
        this.cacheStrategy = msg.cacheStrategy;
        this.receiveDataPort = msg.receiveDataPort;
        this.status = msg.status;
    }

    /**
     * setter
     * */
    public void setPort(int port) { this.receiveDataPort = port; }

    /**
     * setter
     * */
    public void setCacheSize(int cacheSize) { this.cacheSize = cacheSize;}

    /**
     * setter
     * */
    public void setCacheStrategy(String cacheStrategy) { this.cacheStrategy = cacheStrategy; }

    /**
     * setter
     * */
    public void setHost(String host) { this.host = host; }

    /**
     * setter
     * */
    public void setServerTransferProgressStatus(ServerDataTransferProgressStatus status) { this.status = status; }

    /**
     * getter
     * */
    public int getCacheSize(){ return cacheSize; }

    /**
     * getter
     * */
    public String getCacheStrategy(){ return cacheStrategy; }

    /**
     * getter
     * */
    public int getPort() { return receiveDataPort; }

    /**
     * getter
     * */
    public String getHost() { return host; }

    /**
     * getter
     * */
    public ServerDataTransferProgressStatus getStatus() { return status; }
}
