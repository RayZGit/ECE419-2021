package server.Cache;

import app_kvServer.IKVServer;

public class LFUCache extends Cache{
    public LFUCache(int cacheSize, IKVServer kvServer) {
        super(cacheSize, kvServer);
    }
}
