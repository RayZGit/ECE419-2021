package server;

public class ServerMetaData {
    private int cacheSize;
    private String cacheStrategy;
    private int port;
    private int host;
    private ServerDataTransferProgressStatus status;

    public enum ServerDataTransferProgressStatus {
        IN_PROGRESS,       /* If data transfer is in progress */
        IDLE               /* Server status is idle */
    }

    public ServerMetaData(int cacheSize, String cacheStrategy, int port, int host) {
        this.cacheSize = cacheSize;
        this.cacheStrategy = cacheStrategy;
        this.port = port;
        this.host = host;
    }

    /**
     * port setter
     * */
    public void setPort(int port) { this.port = port; }

    public void setCacheSize(int cacheSize) { this.cacheSize = cacheSize;}

    public void setCacheStrategy(String cacheStrategy) { this.cacheStrategy = cacheStrategy; }

    public void setHost(int host) { this.host = host; }

    public void setServerTransferProgressStatus(ServerDataTransferProgressStatus status) { this.status = status; }

    public int getCacheSize(){ return cacheSize; }

    public String getCacheStrategy(){ return cacheStrategy; }

    public int getPort() { return port; }

    public int getHost() { return host; }

    public ServerDataTransferProgressStatus getStatus() { return status; }



}
