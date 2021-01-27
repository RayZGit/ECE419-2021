package server.Cache;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;

import javax.swing.*;
import java.util.Map;

public class Cache implements ICache {
    private int cacheSize;

    private IKVServer kvServer;

    protected Map<String, String> hashmap;

    public Cache(int cacheSize, IKVServer kvServer) {
        this.cacheSize = cacheSize;
        this.kvServer = kvServer;
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
