package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

public class KVServer implements IKVServer, Runnable {

	private static Logger logger = Logger.getRootLogger();

	private int port;
	private int catchSize;
	private CacheStrategy strategy;

	private ServerSocket serverSocket;
	private boolean running;

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
	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		this.port = port;
		this.catchSize = cacheSize;
		this.strategy = CacheStrategy.valueOf(strategy);
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
		return catchSize;
	}

	@Override
	public boolean inStorage(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean inCache(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		return "";
	}

	@Override
	public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
	}

	@Override
	public void clearCache(){
		// TODO Auto-generated method stub
	}

	@Override
	public void clearStorage(){
		// TODO Auto-generated method stub
	}

	@Override
	public void run(){
		System.out.println("in run function");
		running = initializeServer();

		if(serverSocket != null) {
			while(isRunning()){
				try {
					Socket client = serverSocket.accept();
					KVServerConnection connection =
							new KVServerConnection(client);
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
			if(args.length < 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int portNumber = Integer.parseInt(args[0]);
				int catchSize = Integer.parseInt(args[1]);
				String strategy = args[2];
				new Thread(new KVServer(portNumber, catchSize, strategy)).start();
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
			System.out.println("Error! Invalid argument <strategy>! Not one of [None | LRU | LFU | FIFO] ");
			System.exit(1);
		}
	}
}
