package server.Cache;

import java.util.LinkedHashMap;

public class LRUCache implements ICache{

    private LinkedHashMap<String, String> cache;
    LRUCache(int capacity){
        cache = new LinkedHashMap<>(capacity);
    }

    @Override
    public void put(String key, String value) {
        cache.put(key, value);
    }

    @Override
    public String get(String key) {
        return cache.getOrDefault(key, null);
    }

    @Override
    public boolean contain(String key) {
        return cache.containsKey(key);
    }

    @Override
    public void dump(String key) {
        cache.clear();
    }
}
