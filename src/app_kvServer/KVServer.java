package app_kvServer;


import com.google.gson.Gson;
import ecs.ECSNode;
import ecs.HashRing;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import server.Cache.FIFOCache;
import server.Cache.ICache;
import server.Cache.LFUCache;
import server.Cache.LRUCache;
import server.ServerMetaData;
import server.StoreDisk.IStoreDisk;
import server.StoreDisk.StoreDisk;
import shared.messages.KVAdminMessage;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class KVServer implements IKVServer, Runnable, Watcher {

	private static Logger logger = Logger.getRootLogger();

	private int port;
	private int catchSize;
	private CacheStrategy strategy;

	private ServerSocket serverSocket;
	private boolean running;

	private ServerStatus serverStatus;
	private boolean distributed;
	private String serverName;
	public static final String ZK_SERVER_ROOT = "/ZK_KVServers";
	public static final String ZK_METADATA_ROOT = "/KVServer_metadata";

	/**
	 * default */
	private static String defaultCacheStrategy = "LRU";
	private static int defaultCacheSize = 100;
	private static int SESSION_TIME_OUT = 5000;

	/* zookeeper property */
	private String zooKeeperHostName;
	private int zooKeeperPort;
	private ZooKeeper zooKeeper;
	private String zooKeeperPath;

	/* Metadata property*/
	private String serverHashRingStr;

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

	private int cacheSize;

	private ICache cache;
	private IStoreDisk storeDisk;

	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		this.cacheSize = cacheSize;

		this.port = port;
		this.catchSize = cacheSize;
		this.strategy = CacheStrategy.valueOf(strategy);
		this.storeDisk = new StoreDisk("DataDisk"+".txt");
		switch (this.strategy) {
			case FIFO:
				System.out.println("Initialize FIFO");
				this.cache = new FIFOCache(cacheSize, storeDisk);
				break;
			case LRU:
				System.out.println("Initialize LRU");
				this.cache = new LRUCache(cacheSize, storeDisk);
				break;
			case LFU:
				System.out.println("Initialize LFU");
				this.cache = new LFUCache(cacheSize, storeDisk);
				break;
			case None:
				System.out.println("Initialize None Cache");
				this.cache = null;
				break;
		}
	}

	public KVServer(int port, String serverName, String zooKeeperHostName, int zooKeeperPort) {
		this.port = port;
//		this(port, defaultCacheSize, defaultCacheStrategy);
		this.serverName = serverName;
		this.zooKeeperHostName = zooKeeperHostName;
		this.zooKeeperPort = zooKeeperPort;
		this.distributed = true;
		this.serverStatus = ServerStatus.STOP;
		this.zooKeeperPath = ZK_SERVER_ROOT + "/" + serverName; //TODO: zookeeper root path
		String zkConnectionPath = this.zooKeeperHostName + ":" + Integer.toString(this.zooKeeperPort);

		try {
			connectZooKeeper(zkConnectionPath);
		} catch (IOException e) {
			e.printStackTrace();
			logger.debug("Server: " + "<" + this.serverName + ">: " + "Unable to connect to zookeeper");
		}

		try{
			createZKNode(zooKeeperPath);
		} catch (InterruptedException e) {
			logger.debug("Server: " + "<" + this.serverName + ">: " + "create server zNode error");
			e.printStackTrace();
		} catch (KeeperException e) {
			logger.debug("Server: " + "<" + this.serverName + ">: " + "create server zNode error");
			e.printStackTrace();
		}

		try {
			setHashRingInfo();
		} catch (KeeperException e) {
			logger.debug("Server: " + "<" + this.serverName + ">: " + "update meta hash ring error!");
			e.printStackTrace();
		} catch (InterruptedException e) {
			logger.debug("Server: " + "<" + this.serverName + ">: " + "update meta hash ring error!");
			e.printStackTrace();
		}

		try {
			setChildWatcher(zooKeeperPath);
		} catch (KeeperException e) {
			logger.debug("Server: " + "<" + serverName + ">: " + "does not exist, unable to to set child watcher!");
			e.printStackTrace();
		} catch (InterruptedException e) {
			logger.debug("Server: " + "<" + serverName + ">: " + "does not exist, unable to to set child watcher!");
			e.printStackTrace();
		}

		this.storeDisk = new StoreDisk(serverName+ "DataDisk"+".txt");
		switch (this.strategy) {
			case FIFO:
				System.out.println("Initialize FIFO");
				this.cache = new FIFOCache(cacheSize, storeDisk);
				break;
			case LRU:
				System.out.println("Initialize LRU");
				this.cache = new LRUCache(cacheSize, storeDisk);
				break;
			case LFU:
				System.out.println("Initialize LFU");
				this.cache = new LFUCache(cacheSize, storeDisk);
				break;
			case None:
				System.out.println("Initialize None Cache");
				this.cache = null;
				break;
		}

	}

	public void connectZooKeeper(String connectionPath) throws IOException {
		CountDownLatch sig = new CountDownLatch(0);
		this.zooKeeper = new ZooKeeper(connectionPath, SESSION_TIME_OUT, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (event.getState().equals(Event.KeeperState.SyncConnected)) {
					sig.countDown();
				}
			}
		});
		try {
			sig.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void createZKNode(String zkPath) throws KeeperException, InterruptedException {
		if (zooKeeper.exists(zkPath, false) != null) {
			String cache = new String(zooKeeper.getData(zkPath, false, null));
			ServerMetaData metaData = new Gson().fromJson(cache, ServerMetaData.class);
			this.catchSize = metaData.getCacheSize();
			this.strategy = CacheStrategy.valueOf(metaData.getCacheStrategy());
			logger.info("Server: " + "<" + this.serverName + ">: " + "server zNode exist, get cache data and set server cache");
		}
		else{
			byte[] metadata = new Gson().toJson(new ServerMetaData(defaultCacheSize, defaultCacheStrategy, this.port, "localhost")).getBytes();
			zooKeeper.create(zkPath, metadata, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.catchSize = defaultCacheSize;
			this.strategy = CacheStrategy.valueOf(defaultCacheStrategy);
			logger.info("Server: " + "<" + this.serverName + ">: " + "server zNode does not exist, create one");
		}
	}

//	public void checkZKChildren(String zkPath) throws KeeperException, InterruptedException {
//		List<String> children = zooKeeper.getChildren(zkPath, false, null);
//		if (!children.isEmpty()) {
//			String messagePath = zkPath + "/" + children.get(0);
//			byte[] data = zooKeeper.getData(messagePath, false, null);
//			KVAdminMessage message = new Gson().fromJson(new String(data), KVAdminMessage.class);
//			if (message.getFunctionalType().equals(KVAdminMessage.ServerFunctionalType.INIT_KV_SERVER)) {
//				zooKeeper.delete(messagePath, zooKeeper.exists(messagePath, false).getVersion());
//				logger.info("Server: " + "<" + this.serverName + ">: " + "Server initiated ");
//			}
//		}
//	}

	public void setHashRingInfo() throws KeeperException, InterruptedException {
		byte[] hashData = zooKeeper.getData(ZK_METADATA_ROOT, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (running == true) {
					try {
						serverHashRingStr = new String(zooKeeper.getData(ZK_METADATA_ROOT, this, null));
						System.out.println("!!!!!!!!!!!!!!!Hash Ring updated!!!!!!!!!!!!!!!!!!!");
						logger.info("Server: " + "<" + serverName + ">: " + "hash ring updated");
					} catch (KeeperException e) {
						e.printStackTrace();
						logger.debug("Server: " + "<" + serverName + ">: " + "update hash ring error!");
					} catch (InterruptedException e) {
						e.printStackTrace();
						logger.debug("Server: " + "<" + serverName + ">: " + "update hash ring error!");
					}
				}
			}
		}, null);
		System.out.println("!!!!!!!!!!!!!!!Get Hash Ring Info Success!!!!!!!!!!!!!!!!!!!");
		serverHashRingStr = new String(hashData);
	}

	public void setChildWatcher(String zkPath) throws KeeperException, InterruptedException {
		if (zooKeeper.exists(zkPath, false) != null) {
			zooKeeper.getChildren(zkPath, this, null);
		}
		else {
			logger.debug("Server: " + "<" + serverName + ">: " + "does not exist, unable to to set child watcher!");
		}
	}

	@Override
	public int getPort(){
		return port;
	}

	@Override
	public String getHostname(){
		if (serverSocket == null) {
			return null;
		}
		return serverSocket.getInetAddress().getHostName();
	}

	@Override
	public CacheStrategy getCacheStrategy(){
		return strategy;
//		return IKVServer.CacheStrategy.None;
	}

	@Override
	public int getCacheSize(){
		if (cache == null) {
			return 0;
		}
		return cache.getCurrentCacheSize();
	}

	@Override
	public boolean inStorage(String key){
		return storeDisk.contain(key);
	}

	@Override
    public boolean inCache(String key){
		if (cache != null){
			return cache.contain(key);
		}
		return false;
	}

	@Override
    public synchronized String getKV(String key) throws Exception{
		if(key == null){ return null; }
		if(cache == null){
			return storeDisk.get(key);
		}
		if(cache.contain(key)){
			return cache.get(key);
		}
		String val = storeDisk.get(key); // possible return nullptr
		if(val != null){
			cache.put(key,val);
		}
		return val;// possible return nullptr
	}

	@Override
    public synchronized void putKV(String key, String value) throws Exception {
		if (key == null) { return; }
		if(cache == null){
			storeDisk.put(key, value);
			return;
		}
		cache.put(key, value);

	}

	@Override
	public void clearCache(){
		if (cache != null){
			writeCacheToDisk();
			cache.cleanCache();
		}
	}


	@Override
	public synchronized void writeCacheToDisk() {
		if (cache != null) {
			cache.writeCacheToDisk();
		}
	}


	@Override
	public Map<String, String> getCache() {
		if (cache != null) {
			return cache.getMap();
		}
		return null;
	}

	@Override
    public void clearStorage(){
		storeDisk.dump();
	}

	@Override
	public void run(){
//		System.out.println("in run function");
		running = initializeServer();

		if(serverSocket != null) {
			while(isRunning()){
				try {
					Socket client = serverSocket.accept();
					KVServerConnection connection =
							new KVServerConnection(client, this);
					new Thread(connection).start();

					logger.info("<Server> Connected to "
							+ client.getInetAddress().getHostName()
							+  " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("<Server> Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");

	}

	private boolean isRunning() {
		return running;
	}

	@Override
	public boolean isDistributed() {
		return distributed;
	}

	@Override
	public String getServerHashRings(){
		return serverHashRingStr;
	}

	@Override
	public ServerStatus getServerStatus(){
		return serverStatus;
	}

	@Override
	public String getServerName() {
		return serverName;
	}

	@Override
	public void kill(){
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			System.out.println("<Server> Error! " + "Unable to close socket on port: " + port);
			logger.error("<Server> Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	@Override
	public void close(){
		kill();
		clearCache();
	}

	public void delete(String key) throws Exception{
		storeDisk.delete(key, null);
		if (cache != null){
			cache.delete(key);
		}
	}


	private boolean initializeServer() {
		System.out.println("Initialize server");
		logger.info("<Server> Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			System.out.println("Server listening on port: "
					+ serverSocket.getLocalPort());
			logger.info("<Server> Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;

		} catch (IOException e) {
			System.out.println("Error! Cannot open server socket:");
			logger.error("<Server> Error! Cannot open server socket:");
			if(e instanceof BindException){
				System.out.println("Port " + port + " is already bound!");
				logger.error("<Server> Port " + port + " is already bound!");
			}
			return false;
		}
	}

	/**
	 * Main entry point for the echo server application.
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		System.out.println("Start Server");
		try {
			new LogSetup("logs/server.log", Level.ALL);
			if(args.length < 3 || args.length > 4) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port> <cache size> <strategy>!");
				System.err.println("Usage: Server <port> <serverName> <kvHost> <kvPort>!");
			}
			else if (args.length == 3) {
				int portNumber = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String strategy = args[2];
				new Thread(new KVServer(portNumber, cacheSize, strategy)).start();
			}
			else {
				int portNumber = Integer.parseInt(args[0]);
				String serverName = args[1];
				String zkHostName = args[2];
				int zkPort = Integer.parseInt(args[3]);
				new Thread(new KVServer(portNumber, serverName, zkHostName,zkPort)).start();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port> or <catchSize>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		} catch (IllegalArgumentException iae) {
			System.out.println("Error! Invalid argument <strategy>! Should be one of [None | LRU | LFU | FIFO] ");
			System.exit(1);
		} catch (Exception e) {
			System.out.println("Error! Invalid argument number!");
		}
	}

	@Override
	public void process(WatchedEvent watchedEvent) {

	}
}