package server.Cache;

import app_kvServer.IKVServer;

import java.util.LinkedHashMap;
import java.util.Map;

public class FIFOCache extends Cache{
    private final float loadFactor = (float) 0.75;

    /**
     * When parameteraccessOrder = trueWhen, then followAccess order sorts the map, then callget()After the method,
     * the elements of this visit will be moved to the end of the list.
     * Continuous access can form pressesOrder of orderThe linked list.
     *
     * When parameteraccessOrder = falseWhen, then followInsert order sorts the map.
     * The first inserted element is placed in the head of the linked list,
     * and the linked list is maintained in the manner of tail insertion.
     * */
    public FIFOCache(int cacheSize, IKVServer kvServer) {
        super(cacheSize, kvServer);
        this.hashmap = new LinkedHashMap<String, String>(cacheSize, loadFactor, false){
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                //TODO: write eldest to disk before remove the node
                //TODO: check if disk exist, if yes, then continue, if not, write to disk
                return size() > getCacheSize();
            }
        };
    }
}
