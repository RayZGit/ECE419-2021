package server.Cache;

import javax.swing.*;
import java.util.Map;

public class Cache implements ICache {
    private int cacheSize;

    protected Map<String, String> hashmap;

    public Cache(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    @Override
    public void put(String key, String value) {
        hashmap.put(key, value);
    }

    @Override
    public String get(String key) {
        return hashmap.get(key);
    }

    @Override
    public String delete(String key) {
        return hashmap.remove(key);
    }

    @Override
    public boolean contain(String key){
        return hashmap.containsKey(key);
    }

    @Override
    public void dump(String key) {
        hashmap.clear();
    }

    @Override
    public void cleanCache() {
        hashmap.clear();
    }

    @Override
    public void setCacheSize(int size) {
        this.cacheSize = size;
    }

    @Override
    public int getCacheSize() {
        return cacheSize;
    }
}
