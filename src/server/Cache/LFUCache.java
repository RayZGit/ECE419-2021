package server.Cache;

import app_kvServer.IKVServer;
import server.StoreDisk.IStoreDisk;

public class LFUCache extends Cache{
    public LFUCache(int cacheSize, IKVServer kvServer, IStoreDisk storage) {
        super(cacheSize, kvServer, storage);
    }
}
