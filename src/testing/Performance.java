package testing;

import app_kvServer.KVServer;
import client.KVStore;

public class Performance {
    KVServer kvServer;

    public Performance (int port, int cacheSize, String strategy) {
        kvServer = new KVServer(port, cacheSize, strategy);
        new Thread(kvServer).start();
    }

    public void testMultiClientThroughput(int numClient) {
        KVStore kvClient = new KVStore("localhost", 50000);
    }

    public static void main(String[] args) {
        int cacheSize = 200;
        int port = 50000;
        String cacheStrategy = "FIFO";
    }
}
