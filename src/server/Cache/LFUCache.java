package server.Cache;

import app_kvServer.IKVServer;
import server.StoreDisk.IStoreDisk;

public class LFUCache extends Cache{
    public LFUCache(final int cacheSize, final IStoreDisk storage) {
        super(cacheSize, storage);
    }
}
