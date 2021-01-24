package client;

import org.apache.log4j.Logger;
import shared.messages.KVBasicMessage;
import shared.messages.KVMessage;
import shared.messages.KVMsgProtocol;

import java.io.*;
import java.net.Socket;

public class KVStore implements KVCommInterface {
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	private final Logger logger = Logger.getRootLogger();
	private final String address;
	private final int port;
	private Socket socket;
	private InputStream inputStream;
	private OutputStream outputStream;


	public KVStore(String address, int port) {
		// TODO Auto-generated method stub
		this.address = address;
		this.port = port;
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
		KVBasicMessage request = new KVBasicMessage(key, value, KVMessage.StatusType.PUT);
		outputStream.write(KVMsgProtocol.encode(request));
		byte[] buffer = new byte[]
		return KVMsgProtocol.decode(buffer);
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		KVBasicMessage request = new KVBasicMessage(key, null, KVMessage.StatusType.GET);
		outputStream.write(KVMsgProtocol.encode(request));
		return (KVBasicMessage) inputStream.read();
	}

}
