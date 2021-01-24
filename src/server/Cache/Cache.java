package server.Cache;

import javax.swing.*;
import java.util.Map;

public class Cache implements ICache {
    Map<String, String> hashmap;

    @Override
    public void put(String key, String value) {
        hashmap.put(key, value);
    }
    @Override
    public String get(String key) {
        return hashmap.get(key);
    }
    @Override
    public boolean contain(String key){
        return hashmap.containsKey(key);
    }

    @Override
    public void dump(String key) {
        hashmap.clear();
    }
}
