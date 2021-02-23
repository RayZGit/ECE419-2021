package client;

import ecs.ECSNode;
import ecs.HashRing;
import org.apache.log4j.Logger;
import shared.messages.KVBasicMessage;
import shared.messages.KVMessage;
import shared.KVMsgProtocol;

import java.io.*;
import java.net.Socket;

public class KVStore extends KVMsgProtocol implements KVCommInterface {
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	private String address;
	private int port;
	private Socket socket;
	private final int LEN_KEY = 20;
	private final int LEN_VALUE = 120 * 1024;
	private HashRing hashRing;


	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		this.address = address;
		this.port = port;
		hashRing = new HashRing();
		ECSNode node = new ECSNode("firstnode", this.address, this.port);
		hashRing.addNode(node);
	}

	@Override
	public void connect() throws Exception {
		// TODO Auto-generated method stub
		socket = new Socket(address, port);
		System.out.println("Connected to: " + address + " : " + port);
		logger.info("Connected to: " + address + " : " + port);
		inputStream = socket.getInputStream();
		outputStream = socket.getOutputStream();
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		if (socket != null){
			try {
				socket.close();
			} catch (IOException e){
				System.out.println("Disconnect failed: unable to close the socket");
				logger.error("Disconnect failed: unable to close the socket");
			}
		}
		socket = null;
		try {
			inputStream.close();
			outputStream.close();
		} catch (IOException e){
			System.out.println("Disconnect failed: unable to close the stream");
			logger.error("Disconnect failed: unable to close the stream");
		}
		System.out.println("Socket disconnected.");
		logger.info("Socket disconnected.");
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		if (key.length() > LEN_KEY) {
			throw new Exception("Put failed: Key too long");
		}
		if (value.length() > LEN_VALUE) {
			throw new Exception("Put failed: Value too long");
		}
		KVBasicMessage request = new KVBasicMessage(key, value, KVMessage.StatusType.PUT);
		try{
			updateServer(request);
			sendMessage(request);
			KVMessage response = receiveMessage();
			if (response.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
				hashRing = new HashRing(response.getValue());
				return put(key, value);
			}
			return response;
		} catch (IOException e) {
			logger.warn("Detect Server down, try to remove the failed server.");
			handleShutdown(request);
			return put(key, value);
		}
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		if (key.length() > LEN_KEY) {
			throw new Exception("Get failed: Key too long");
		}
		KVBasicMessage request = new KVBasicMessage(key, null, KVMessage.StatusType.GET);
		try {
			updateServer(request);
			sendMessage(request);
			KVMessage response = receiveMessage();
			if (response.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
				hashRing = new HashRing(response.getValue());
				return get(key);
			}
			return response;
		} catch (IOException e) {
			logger.warn("Detect Server down, try to remove the failed server.");
			handleShutdown(request);
			return get(key);
		}
	}

	private void handleShutdown(KVBasicMessage request) throws Exception{
		disconnect();
		String hash = HashRing.getHash(request.getKey());
		hashRing.removeNode(hashRing.getNodeByKey(hash));
		ECSNode node = hashRing.getNodeByKey(hash);
		if (node == null) {
			logger.error("No Server available.");
			throw new Exception("No Server available.");
		}
	}

	@Override
	public KVMessage delete(String key) throws Exception {
		if (key.length() > LEN_KEY) {
			throw new Exception("Put failed: Key too long");
		}
		KVBasicMessage request = new KVBasicMessage(key, null, KVMessage.StatusType.DELETE);
		try {
			updateServer(request);
			sendMessage(request);
			KVMessage response = receiveMessage();
			if (response.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
				hashRing = new HashRing(response.getValue());
				return delete(key);
			}
			return response;
		} catch (IOException e) {
			logger.warn("Detect Server down, try to remove the failed server.");
			handleShutdown(request);
			return delete(key);
		}
	}


	public void updateServer(KVBasicMessage request) throws Exception{
		ECSNode node = hashRing.getNodeByKey(HashRing.getHash(request.getKey()));
		if (!address.equals(node.getNodeHost()) || port != node.getNodePort()) {
			address = node.getNodeHost();
			port = node.getNodePort();
			disconnect();
			connect();
			logger.info("Connect to the server " + address + ":" + port);
		}
	}

}
