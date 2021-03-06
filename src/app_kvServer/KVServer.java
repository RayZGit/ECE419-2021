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
import server.Cache.*;
import server.ServerMetaData;
import server.StoreDisk.IStoreDisk;
import server.StoreDisk.StoreDisk;
import shared.messages.KVAdminMessage;

import java.io.*;
import java.lang.reflect.Type;
import java.net.BindException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

public class KVServer implements IKVServer, Runnable, Watcher {

	private static final int BUFFER_SIZE = 64;
	private static Logger logger = Logger.getRootLogger();

	private int port;
	private int catchSize;
	private CacheStrategy strategy;

	private ServerSocket serverSocket;
	private ServerSocket receiverSocket;

	private boolean running;
	private boolean isReceiverRunning;

	private ServerStatus serverStatus;
	private boolean distributed = false;
	private String serverName;
	private ServerMetaData serverMetaData;
	public static final String ZK_SERVER_ROOT = "/ZNode";
	public static final String ZK_METADATA_ROOT = "/MD";
	private static final String ZNODE_KVMESSAGE = "/KVAdminMessage";
	private static final int RECEIVE_DATA_PORT = 0;

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
	private String diskFileName;

	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		this.cacheSize = cacheSize;

		this.port = port;
		this.catchSize = cacheSize;
		this.strategy = CacheStrategy.valueOf(strategy);
		this.diskFileName = "DataDisk";
		this.storeDisk = new StoreDisk(this.diskFileName);
		this.serverStatus = ServerStatus.START;
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
		System.out.println("------------------------In KVServer distributed constructor, Server Name: " + serverName);
		assert(port != RECEIVE_DATA_PORT);
		this.port = port;
//		this(port, defaultCacheSize, defaultCacheStrategy);
		this.serverName = serverName;
		this.zooKeeperHostName = zooKeeperHostName;
		this.zooKeeperPort = zooKeeperPort;
		this.distributed = true;
		this.serverStatus = ServerStatus.STOP;
		this.serverMetaData = new ServerMetaData(0, "None", RECEIVE_DATA_PORT, "127.0.0.1");
		this.zooKeeperPath = ZK_SERVER_ROOT + "/" + serverName; //TODO: zookeeper root path
		String zkConnectionPath = this.zooKeeperHostName + ":" + Integer.toString(this.zooKeeperPort);

		try {
			connectZooKeeper(zkConnectionPath);

			try{
				if (this.zooKeeper != null) {
					createZKNode(zooKeeperPath);
					logger.info("zooKeeper connection established ...");
				}
			} catch (InterruptedException e) {
				logger.debug("Server: " + "<" + this.serverName + ">: " + "create server zNode error");
				e.printStackTrace();
			} catch (KeeperException e) {
				logger.debug("Server: " + "<" + this.serverName + ">: " + "create server zNode error");
				e.printStackTrace();
			}

		} catch (IOException e) {
			e.printStackTrace();
			logger.debug("Server: " + "<" + this.serverName + ">: " + "Unable to connect to zookeeper");
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
			setServerNodeWatcher(zooKeeperPath);
		} catch (KeeperException e) {
			logger.debug("Server: " + "<" + serverName + ">: " + "does not exist, unable to set server Znode watcher!");
			e.printStackTrace();
		} catch (InterruptedException e) {
			logger.debug("Server: " + "<" + serverName + ">: " + "does not exist, unable to set server Znode watcher!");
			e.printStackTrace();
		}

		try {
			setMetaDataWatcher(ZK_METADATA_ROOT);
		} catch (KeeperException e) {
			e.printStackTrace();
			logger.debug("Server: " + "<" + serverName + ">: " + "does not exist, unable to set server meta data watcher!");
		} catch (InterruptedException e) {
			e.printStackTrace();
			logger.debug("Server: " + "<" + serverName + ">: " + "does not exist, unable to set server meta data watcher!");
		}

		try {
			zooKeeper.getChildren(this.zooKeeperPath, this, null);
			logger.debug("Server: " + "<" + serverName + ">: " + "Set up watcher on " + this.zooKeeperPath);
		} catch (InterruptedException | KeeperException e) {
			logger.error("Server: " + "<" + serverName + ">: " + "Unable to get set watcher on children");
			e.printStackTrace();
		}

		this.diskFileName =  "DataDisk_" + serverName;
		this.storeDisk = new StoreDisk(this.diskFileName);

		System.out.println("Cache strategy: " + this.strategy);
		System.out.println("Cache Size: " + this.catchSize);
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
		final CountDownLatch countDownSign = new CountDownLatch(0);
		zooKeeper = new ZooKeeper(connectionPath, SESSION_TIME_OUT, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (event.getState().equals(Event.KeeperState.SyncConnected)) {
					countDownSign.countDown();
				}
			}
		});
		try {
			countDownSign.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	// from: https://www.jianshu.com/p/44c3d73dc30d
	public void createZKNode(String zkPath) throws KeeperException, InterruptedException {
		if (zooKeeper.exists(zkPath, false) != null) {
			String cache = new String(zooKeeper.getData(zkPath, false, null));
			ServerMetaData metaData = new Gson().fromJson(cache, ServerMetaData.class);
			this.catchSize = metaData.getCacheSize();
			this.strategy = CacheStrategy.valueOf(metaData.getCacheStrategy());
			zooKeeper.setData(zkPath , null, zooKeeper.exists(zkPath, false).getVersion());
//			zooKeeper.exists(zkPath, false);
			logger.info("Server: " + "<" + this.serverName + ">: " + "server zNode exist, get cache data and set server cache");
		}
		else{
			byte[] metadata = new Gson().toJson(new ServerMetaData(defaultCacheSize, defaultCacheStrategy, this.port, "localhost")).getBytes();
			zooKeeper.create(zkPath, metadata, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			this.catchSize = defaultCacheSize;
			this.strategy = CacheStrategy.valueOf(defaultCacheStrategy);
			System.out.println("Server: " + "<" + this.serverName + ">: " + "server zNode does not exist, create one");
			logger.info("Server: " + "<" + this.serverName + ">: " + "server zNode does not exist, create one");
		}
		serverMetaData.setCacheSize(this.catchSize);
		serverMetaData.setCacheStrategy(this.strategy.toString());
	}

	public void setHashRingInfo() throws KeeperException, InterruptedException {
		byte[] hashData = zooKeeper.getData(ZK_METADATA_ROOT, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (running == true) {
					try {
						serverHashRingStr = new String(zooKeeper.getData(ZK_METADATA_ROOT, this, null));
						zooKeeper.exists(ZK_METADATA_ROOT, this);
						System.out.println("!!!!!!!!!!!!!!! In Process, Hash Ring updated, hashring is: " + serverHashRingStr);
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
		System.out.println("!!!!!!!!!!!!!!!  Hash Ring updated, hashring is: " + serverHashRingStr);
	}

	public void setServerNodeWatcher(String zkPath) throws KeeperException, InterruptedException {
		if (zooKeeper.exists(zkPath, false) != null) {
			zooKeeper.getData(zkPath, this, null);
		}
		else {
			logger.debug("Server: " + "<" + serverName + ">: " + "does not exist, unable to to set server zNode watcher!");
		}
	}

	public void setMetaDataWatcher(String zkPath) throws KeeperException, InterruptedException {
		if (zooKeeper.exists(zkPath, false) != null) {
			zooKeeper.getData(zkPath, this, null);
		}
		else {
			logger.debug("Server: " + "<" + serverName + ">: " + "does not exist, unable to to set metaData watcher!");
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
	public String getServerDiskFile() {
		return storeDisk.getResourceDir() + this.diskFileName;
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

	private boolean isReceiverRunning() { return isReceiverRunning; }

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

	private boolean initializeReceiverServer() {
		System.out.println("Initialize receiver server----------------------------------------");
		logger.info("<Receiver Server> Initialize server ...");
		try {
			receiverSocket = new ServerSocket(RECEIVE_DATA_PORT);
			serverMetaData.setPort(receiverSocket.getLocalPort());
			System.out.println("Receiver Server listening on port: "
					+ receiverSocket.getLocalPort() + receiverSocket.getLocalPort() + receiverSocket.getLocalPort() + receiverSocket.getLocalPort());
			logger.info("<Receiver Server> Server listening on port: "
					+ receiverSocket.getLocalPort());
			System.out.println("Initialize receiver server DONE!!!!!!!!!!!!!!!!!!!!!!!!----------------------------------------");
			return true;

		} catch (IOException e) {
			System.out.println("Error! Cannot open Receiver server socket:");
			logger.error("<Receiver Server> Error! Cannot open Receiver server socket:");
			if(e instanceof BindException){
				System.out.println("Receiver Server Port " + port + " is already bound!");
				logger.error("<Receiver Server> Port " + port + " is already bound!");
			}
			return false;
		}
	}

	public int receiveServerData(KVAdminMessage request, String path) {

		System.out.println("Receiver Data 1" );
		isReceiverRunning = initializeReceiverServer();
		System.out.println("---------------------------------------I am receiving on port: " + port);
		request.setReceiveServerPort(receiverSocket.getLocalPort());
		try{
			zooKeeper.setData(path , request.encode().getBytes(), zooKeeper.exists(path, false).getVersion());
			logger.info("$$$$$$$$$$$$"+request.toString()+"RAY WOULD LIKE TO SEE");
			zooKeeper.exists(path, this);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}


		System.out.println("Receiver Data 1.1" );
		if(receiverSocket != null) {
			System.out.println("Receiver Data 1.2" );

				System.out.println("Receiver Data 1.3" );
				try {
					Socket client = receiverSocket.accept();
					System.out.println("Receiver Data 1.4" );

					lockWrite();
					this.serverMetaData.setServerTransferProgressStatus(ServerMetaData.ServerDataTransferProgressStatus.IN_PROGRESS);
					try {
						zooKeeper.setData(zooKeeperPath, this.serverMetaData.encode().getBytes(), zooKeeper.exists(zooKeeperPath, false).getVersion());
//						zooKeeper.exists(zooKeeperPath, this);
						logger.info("Server: " + "<" + this.serverName + ">: " + "Receiver Data, update server data transfer progress to IN_PROGRESS");
					} catch (InterruptedException e) {
						e.printStackTrace();
						System.out.println("Receiver Data 5" );
						logger.error("ERROR! Server: " + "<" + this.serverName + ">: " + "Receiver Data, update server data transfer progress to IN_PROGRESS");
					} catch (KeeperException e) {
						e.printStackTrace();
						System.out.println("Receiver Data 6" );
						logger.error("ERROR! Server: " + "<" + this.serverName + ">: " + "Receiver Data,update server data transfer progress to IN_PROGRESS");
					}

					new Thread(new KVServerDataTransferConnection(client, this)).start();

					logger.info("<Receiver Server> Connected to "
							+ client.getInetAddress().getHostName()
							+  " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("<Receiver Server> Error! " +
							"Unable to establish connection. \n", e);
				}
			}

		logger.info("Receiver Server stopped.");

		this.serverMetaData.setServerTransferProgressStatus(ServerMetaData.ServerDataTransferProgressStatus.IDLE);
		try{
			zooKeeper.setData(zooKeeperPath , this.serverMetaData.encode().getBytes(), zooKeeper.exists(zooKeeperPath, false).getVersion());
//				zooKeeper.exists(zooKeeperPath, this);
			logger.info("Server: " + "<" + this.serverName + ">: " + "Receiver Data, update server data transfer progress to IDLE");
		} catch (InterruptedException e) {
			e.printStackTrace();
			logger.error("ERROR! Server: " + "<" + this.serverName + ">: " + "Receiver Data, update server data transfer progress to IDLE");
		} catch (KeeperException e) {
			e.printStackTrace();
			logger.error("ERROR! Server: " + "<" + this.serverName + ">: " + "Receiver Data, update server data transfer progress to IDLE");
		}
		System.out.println("Receiver Server port: " + receiverSocket.getLocalPort());
		return receiverSocket.getLocalPort();


//			receiverSocket = new ServerSocket(0);
//			int recei_port = receiverSocket.getLocalPort();
//			System.out.println("Transfer Data Server listening on port: " + recei_port);

//			logger.info("Transfer Data Server listening on port: " + recei_port);
//
//			int port = receiverSocket.getLocalPort();
//			System.out.println("Receiver Data 2" );
//			lockWrite();
//			this.serverMetaData.setServerTransferProgressStatus(ServerMetaData.ServerDataTransferProgressStatus.IN_PROGRESS);
//			System.out.println("Receiver Data 3" );
//			try {
//				zooKeeper.setData(zooKeeperPath, this.serverMetaData.encode().getBytes(), zooKeeper.exists(zooKeeperPath, false).getVersion());
////				zooKeeper.exists(zooKeeperPath, this);
//				System.out.println("Receiver Data 4" );
//				logger.info("Server: " + "<" + this.serverName + ">: " + "Receiver Data, update server data transfer progress to IN_PROGRESS");
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//				System.out.println("Receiver Data 5" );
//				logger.error("ERROR! Server: " + "<" + this.serverName + ">: " + "Receiver Data, update server data transfer progress to IN_PROGRESS");
//			} catch (KeeperException e) {
//				e.printStackTrace();
//				System.out.println("Receiver Data 6" );
//				logger.error("ERROR! Server: " + "<" + this.serverName + ">: " + "Receiver Data,update server data transfer progress to IN_PROGRESS");
//			}
//			new Thread(new KVServerDataTransferConnection(receiverSocket, this)).start();

//			this.serverMetaData.setServerTransferProgressStatus(ServerMetaData.ServerDataTransferProgressStatus.IDLE);
//			try{
//				zooKeeper.setData(zooKeeperPath , this.serverMetaData.encode().getBytes(), zooKeeper.exists(zooKeeperPath, false).getVersion());
////				zooKeeper.exists(zooKeeperPath, this);
//				logger.info("Server: " + "<" + this.serverName + ">: " + "Receiver Data, update server data transfer progress to IDLE");
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//				logger.error("ERROR! Server: " + "<" + this.serverName + ">: " + "Receiver Data, update server data transfer progress to IDLE");
//			} catch (KeeperException e) {
//				e.printStackTrace();
//				logger.error("ERROR! Server: " + "<" + this.serverName + ">: " + "Receiver Data, update server data transfer progress to IDLE");
//			}
//
//			return receiverPortNumber;
//		} catch (IOException e) {
//			logger.debug("Server: " + "<" + this.serverName + ">: " + "Unable to open a receiver socket!");
//			e.printStackTrace();
//			return 0; // this exception should be handled by ecs
//		}

	}

	@Override
	public void lockWrite() {
		this.serverStatus = serverStatus.LOCK;
		logger.info("Server: " + "<" + this.serverName + ">: " + "Server status change to WRITE_LOCK");
	}

	@Override
	public void unlockWrite() {
		this.serverStatus = serverStatus.UNLOCK;
		logger.info("Server: " + "<" + this.serverName + ">: " + "Server status change to UnLOCK");
	}

	public void moveData(String[] hashRange, String host, int port) {
		System.out.println("------------In move data, host is " + host + " ||||| port is: " + port);
		this.serverMetaData.setServerTransferProgressStatus(ServerMetaData.ServerDataTransferProgressStatus.IN_PROGRESS);
		try {
//			String zooKeeperPath = ZK_SERVER_ROOT + "/" + serverName;
//			byte[] serverData = zooKeeper.getData(zooKeeperPath,false, null);
//			ServerMetaData data = new Gson().fromJson(serverData.toString(), ServerMetaData.class);
			System.out.println("In move data 1, zNode path: " + zooKeeperPath);
			zooKeeper.setData(zooKeeperPath, this.serverMetaData.encode().getBytes(), zooKeeper.exists(zooKeeperPath, false).getVersion());
//			zooKeeper.exists(zooKeeperPath, this);
			logger.info("Server: " + "<" + this.serverName + ">: " + "Send Data, update server data transfer progress to IN_PROGRESS");
		} catch (KeeperException e) {
			e.printStackTrace();
			logger.error("ERROR! Server: " + "<" + this.serverName + ">: " + "Send Data, update server data transfer progress to IN_PROGRESS");
		} catch (InterruptedException e) {
			e.printStackTrace();
			logger.error("ERROR! Server: " + "<" + this.serverName + ">: " + "Send Data, update server data transfer progress to IN_PROGRESS");
		}
		lockWrite();
		writeCacheToDisk();
		try{
			System.out.println("------------In move data 1----------------");
			File toMove = storeDisk.filter(hashRange);
			System.out.println("------------In move data 2----------------");
			long fileLen = toMove.length();
			System.out.println(host + port);
			Socket socket;
			while (true) {
				try {
					socket = new Socket(host, port);
					break;
				} catch (ConnectException e) {
					continue;
				}
			}
			System.out.println("------------In move data 3----------------");
			BufferedOutputStream outputStream = new BufferedOutputStream(socket.getOutputStream());
			System.out.println("------------In move data 4----------------");
			BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(toMove));
			System.out.println("------------In move data 5----------------");

			byte[] buffer = new byte[BUFFER_SIZE];
			int len = 0;
			int current = 0;
			int percentage = 0;
			while ((len = inputStream.read(buffer)) > 0) {
				percentage = (int) (current * 100 / fileLen);
				System.out.println("Transfer progress: " + percentage);
				//TODO: updata progress
				outputStream.write(buffer, 0, len);
				current += len;
			}
			System.out.println("------------In move data 6----------------");
			inputStream.close();
			outputStream.close();
			socket.close();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("fail to move data to new server.");
		} finally {
			unlockWrite();
			this.serverMetaData.setServerTransferProgressStatus(ServerMetaData.ServerDataTransferProgressStatus.IDLE);
			try {
				System.out.println("In move data 2 , zNode path: " + zooKeeperPath);
				zooKeeper.setData(zooKeeperPath , this.serverMetaData.encode().getBytes(), zooKeeper.exists(zooKeeperPath, false).getVersion());
//				zooKeeper.exists(zooKeeperPath, this);
				logger.info("Server: " + "<" + this.serverName + ">: " + "Send Data, update server data transfer progress to IDLE");
			} catch (KeeperException e) {
				e.printStackTrace();
				logger.error("ERROR! Server: " + "<" + this.serverName + ">: " + "Send Data, update server data transfer progress to IDLE");
			} catch (InterruptedException e) {
				e.printStackTrace();
				logger.info("ERROR! Server: " + "<" + this.serverName + ">: " + "Send Data, update server data transfer progress to IDLE");
			}
			//TODO: set server status to IDLE
		}
	}

	@Override
	public void process(WatchedEvent watchedEvent) {
		if (running) {
			//https://juejin.cn/post/6850037265213685768
			//Type
			Event.EventType eventType = watchedEvent.getType();
			//Status
			Event.KeeperState eventState = watchedEvent.getState();
			//Path
			String eventPath = watchedEvent.getPath();
			System.out.println("Event Type:" + eventType.name());
			System.out.println("Event Status:" + eventState.name());
			System.out.println("Event ZNode path:" + eventPath);

			try {
				String path = zooKeeperPath + ZNODE_KVMESSAGE;
				byte[] zNodeData = zooKeeper.getData(path , false, null);
				String temp = new String(zNodeData);
				KVAdminMessage request = new Gson().fromJson(temp, KVAdminMessage.class);
				KVAdminMessage.ServerFunctionalType serverType = request.getFunctionalType();
				//TODO: set data to null, let ECS know process is done
				System.out.println(" \n !!!!!!!! In KVServer Process, Type is " + serverType);
				System.out.println("Server ZNode path is: " + path);
				switch (serverType) {
					case INIT_KV_SERVER:
						this.serverStatus = ServerStatus.INITIALIZED;
						zooKeeper.setData(path , "INIT_ACK".getBytes(), zooKeeper.exists(path, false).getVersion());
						zooKeeper.exists(path, this);
						logger.info("Server: " + "<" + serverName + ">: "+ "Server initiated but stop processing requests");
						break;
					case START:
						this.serverStatus = ServerStatus.START;
						zooKeeper.setData(path , "START_ACK".getBytes(), zooKeeper.exists(path, false).getVersion());
						zooKeeper.exists(path, this);
						logger.info("Server: " + "<" + serverName + ">: "+ "Server Started");
						break;
					case STOP:
						this.serverStatus = ServerStatus.STOP;
						zooKeeper.setData(path , "STOP_ACK".getBytes(), zooKeeper.exists(path, false).getVersion());
						zooKeeper.exists(path, this);
						logger.info("Server: " + "<" + serverName + ">: "+ "Server Stopped");
						break;
					case SHUT_DOWN:
						this.serverStatus = ServerStatus.SHOT_DOWN;
						zooKeeper.setData(path , "SHUT_DOWN_ACK".getBytes(), zooKeeper.exists(path, false).getVersion());
						zooKeeper.exists(path, this);
						logger.info("Server: " + "<" + serverName + ">: "+ "Server ShotDown");
						close();
						break;
					case LOCK_WRITE:
						this.serverStatus = ServerStatus.LOCK;
						zooKeeper.setData(path , "LOCK_WRITE_ACK".getBytes(), zooKeeper.exists(path, false).getVersion());
						zooKeeper.exists(path, this);
						logger.info("Server: " + "<" + serverName + ">: "+ "Server Locked");
						break;
					case UNLOCK_WRITE:
						this.serverStatus = ServerStatus.UNLOCK;
						zooKeeper.setData(path , "UNLOCK_WRITE_ACK".getBytes(), zooKeeper.exists(path, false).getVersion());
						zooKeeper.exists(path, this);
						logger.info("Server: " + "<" + serverName + ">: "+ "Server unLocked");
						break;
					case RECEIVE:
						System.out.println(" \n !!!!!!!! RECEIVE RECEIVE RECEIVE !!!!!");
						logger.info("Server: " + "<" + serverName + ">: "+ "receiving data initialization....");
						receiveServerData(request, path);
						zooKeeper.setData(path , "RECEIVE_ACK".getBytes(), zooKeeper.exists(path, false).getVersion());
						zooKeeper.exists(path, this);
						break;
					case MOVE_DATA:
						System.out.println(" \n !!!!!!!! MOVE_DATA MOVE_DATA MOVE_DATA !!!!!");
						logger.info("Server: " + "<" + serverName + ">: "+ "moving data initialization....");
//						moveData(request.getReceiveHashRangeValue(), request.getReceiverHost(), request.getReceiveServerPort());
						zooKeeper.setData(path , "MOVE_DATA_ ACK".getBytes(), zooKeeper.exists(path, false).getVersion());
						zooKeeper.exists(path, this);
						break;
					case UPDATE:
						this.serverHashRingStr = request.getHashRingStr();
						zooKeeper.setData(path , "UPDATE_ACK".getBytes(), zooKeeper.exists(path, false).getVersion());
						zooKeeper.exists(path, this);
						logger.info("Server: " + "<" + serverName + ">: " + "Server updated meta data");
						break;
				}

			} catch (KeeperException e) {
				logger.debug("Server: " + "<" + serverName + ">: " + "Unable to process the watcher event");
				e.printStackTrace();
			} catch (InterruptedException e) {
				logger.debug("Server: " + "<" + serverName + ">: " + "Unable to process the watcher event");
				e.printStackTrace();
			}

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
			e.printStackTrace();
		}
	}
}