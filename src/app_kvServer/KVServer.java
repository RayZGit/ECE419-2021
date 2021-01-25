package app_kvServer;

import server.Cache.ICache;
import server.StoreDisk.IStoreDisk;
import server.StoreDisk.StoreDisk;

public class KVServer implements IKVServer {
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */

	private CacheStrategy strategy;
	private int cacheSize;

	private ICache cache;
	private IStoreDisk storeDisk;

	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		this.cacheSize = cacheSize;
		this.storeDisk = new StoreDisk(String.valueOf(port));

	}
	
	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		return -1;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		return null;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		return IKVServer.CacheStrategy.None;
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return -1;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public boolean inCache(String key){
		return false;
	}

	@Override
    public String getKV(String key) throws Exception{
		if(key == null){
			return null;
		}
		return storeDisk.get(key);
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		if(key != null){
			storeDisk.put(key,value);
		}

	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
	}

	@Override
    public void clearStorage(){
		storeDisk.dump();
	}

	@Override
    public void run(){
		// TODO Auto-generated method stub
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
	}
}
