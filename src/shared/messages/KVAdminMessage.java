package shared.messages;

import com.google.gson.Gson;
import ecs.HashRing;

import java.util.Arrays;

public class KVAdminMessage {

    private String receiveServerName = null;
    private String receiveServerHost = null;
    private int receiveServerPort;
    private String[] receiveHashRangeValue = null;
    private ServerFunctionalType serverFunctionalType;
    private HashRing hashRing;
    private String hashRingStr;

    public enum ServerFunctionalType {
        INIT_KV_SERVER,       /* Used by awaitNodes, Initialize the KVServer with the metadata and block it for client requests */
        START,                /* Starts the KVServer, all client requests and all ECS requests are processed. */
        STOP,                 /* Stops the KVServer, all client requests are rejected and only ECS requests are processed.*/
        SHUT_DOWN,            /* Exits the KVServer application. */
        LOCK_WRITE,           /* Lock the KVServer for write operations.*/
        UNLOCK_WRITE,         /* Unlock the KVServer for write operations. */
        RECEIVE,              /* server receive a range of the KVServer's data*/
        MOVE_DATA,            /* transfer a subset (range) of the KVServer’s data to another KVServer *///TODO: SEND
        UPDATE,               /* Update the metadata repository of this server */
    }



    public KVAdminMessage(ServerFunctionalType operationType) {
        this.serverFunctionalType = operationType;
    }

    public ServerFunctionalType getFunctionalType() {
        return serverFunctionalType;
    }

    public String getReceiverName() {
        return receiveServerName;
    }

    public String getReceiverHost() {
        return receiveServerHost;
    }

    public int getReceiveServerPort() {
        return receiveServerPort;
    }

    public String[] getReceiveHashRangeValue() {
        return receiveHashRangeValue;
    }

    public String getHashRingStr() { return hashRingStr; }

    public HashRing getHashRing() { return hashRing; }

    public void setReceiverName(String name) {
        this.receiveServerName = name;
    }

    public void setReceiverHost(String host) {
        this.receiveServerHost = host;
    }

    public void setReceiveServerPort(int port) {
        this.receiveServerPort = port;
    }

    public void setReceiveHashRangeValue(String[] hashValue) {
        this.receiveHashRangeValue = hashValue;
    }

    public void setHashRingStr(String hashRingData) {this.hashRingStr = hashRingData; }

    public void setHashRing(HashRing hashRingData) {this.hashRing = hashRingData; }

    @Override
    public String toString() {
        return "KVAdminMessage{" +
                "receiveServerName='" + receiveServerName +
                ", serverFunctionalType=" + serverFunctionalType  + '\'' +
                ", receiveServerHost='" + receiveServerHost + '\'' +
                ", receiveServerPort='" + receiveServerPort + '\'' +
                ", hashRangeValue=" + Arrays.toString(receiveHashRangeValue) +
                '}';
    }

    public String encode() {
        return new Gson().toJson(this);
    }

    public void decode(String input) {
        KVAdminMessage msg = new Gson().fromJson(input, this.getClass());
        this.receiveServerName = msg.receiveServerName;
        this.receiveServerHost = msg.receiveServerHost;
        this.receiveHashRangeValue = msg.receiveHashRangeValue;
        this.receiveServerPort = msg.receiveServerPort;
        this.serverFunctionalType = msg.serverFunctionalType;
    }
}
