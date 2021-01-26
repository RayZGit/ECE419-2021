package server.Cache;

import app_kvServer.IKVServer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * https://www.programmersought.com/article/6678892876/
 * */

public class LRUCache extends Cache{
    private final float loadFactor = (float) 0.75;
    public LRUCache(int capacity, IKVServer kvServer){
        super(capacity, kvServer);
        this.hashmap = new LinkedHashMap<String, String>(capacity, loadFactor, true){
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                //TODO: write eldest to disk before remove the node
                //TODO: check if disk exist, if yes, then continue, if not, write to disk
                return size() > getCacheSize();
            }
        };
    }


//    @Override
//    public void put(String key, String value) {
//        cache.put(key, value);
//    }
//
//    @Override
//    public String get(String key) {
//        return cache.getOrDefault(key, null);
//    }
//
//    @Override
//    public boolean contain(String key) {
//        return cache.containsKey(key);
//    }
//
//    @Override
//    public void dump(String key) {
//        cache.clear();
//    }
}
