package server.Cache;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;
import server.StoreDisk.IStoreDisk;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * https://www.programmersought.com/article/6678892876/
 * */

public class LRUCache extends Cache{

    private final float loadFactor = (float) 0.75;
    private static Logger logger = Logger.getRootLogger();

    public LRUCache(final int cacheSize, final IStoreDisk storage){
        super(cacheSize,storage);
        this.hashmap = new LinkedHashMap<String, String>(cacheSize, loadFactor, true){
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                //TODO: write eldest to disk before remove the node
                //TODO: check if disk exist, if yes, then continue, if not, write to disk
                boolean isMaxCapacity = size() > cacheSize;
                if (isMaxCapacity) {
                    try {
                        storage.put(eldest.getKey(), eldest.getValue());
                        System.out.println("<LRU> (" + eldest.getKey() + eldest.getValue() + ") move eldest KV to disk");
                        logger.info("<LRU> (" + eldest.getKey() + eldest.getValue() + ") move eldest KV to disk");
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.error("<Server> Cache write disk error!");
                        System.out.println("Cache write disk error!");
                    }
                }
                return isMaxCapacity;
            }
        };
    }
}
