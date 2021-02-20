package server;

public class ServerMetaData {
    private int cacheSize;
    private String cacheStrategy;
    private int port;
    private String host;
    private ServerDataTransferProgressStatus status;

    public enum ServerDataTransferProgressStatus {
        IN_PROGRESS,       /* If data transfer is in progress */
        IDLE               /* Server status is idle */
    }

    public ServerMetaData(int cacheSize, String cacheStrategy, int port, String host) {
        this.cacheSize = cacheSize;
        this.cacheStrategy = cacheStrategy;
        this.port = port;
        this.host = host;
    }

    /**
     * setter
     * */
    public void setPort(int port) { this.port = port; }

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
    public int getPort() { return port; }

    /**
     * getter
     * */
    public String getHost() { return host; }

    /**
     * getter
     * */
    public ServerDataTransferProgressStatus getStatus() { return status; }
}
