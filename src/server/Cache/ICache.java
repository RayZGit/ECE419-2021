package server.Cache;

import java.util.Map;

public interface ICache {
    public void put(String key, String value);
    public String get(String key);
    public String delete(String key);
    public boolean contain(String key);
    public void cleanCache();
    public void setCacheSize(int size);
    public int getCurrentCacheSize();
    public void writeCacheToDisk();
    public Map<String, String> getMap();
}
