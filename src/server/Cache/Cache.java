package server.Cache;

import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import org.apache.log4j.Logger;
import server.StoreDisk.IStoreDisk;

import javax.swing.*;
import java.util.Map;

public class Cache implements ICache {
    private int cacheSize;

//    private IKVServer kvServer;
    private IStoreDisk storage;

    protected Map<String, String> hashmap;


    public Cache(int cacheSize, IStoreDisk storage) {
        this.cacheSize = cacheSize;
//        this.kvServer = kvServer;
        this.storage = storage;
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
    public void cleanCache() { hashmap.clear(); }

    @Override
    public  void writeCacheToDisk(){
        for (Map.Entry<String,String> entry : hashmap.entrySet()) {
            try {
                storage.put(entry.getKey(),entry.getValue());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void setCacheSize(int size) {
        this.cacheSize = size;
    }

    @Override
    public int getCurrentCacheSize() {
        return hashmap.size();
    }

    @Override
    public Map<String, String> getMap(){
        return hashmap;
    }
}
