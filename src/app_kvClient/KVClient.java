package app_kvClient;

import client.KVStore;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import client.KVCommInterface;
import shared.messages.KVMessage;

import java.io.BufferedReader;
import java.io.IOException;

public class KVClient implements IKVClient, Runnable {

    private Logger logger = Logger.getRootLogger();
    private BufferedReader input;

    private KVStore KVClientServer;
    private boolean running;

    @Override
    public void run(){
        // TODO Auto-generated method stub
        running = true;
    }
    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub
        if (KVClientServer != null){
            throw new IOException();
        }
        KVClientServer = new KVStore(hostname, port);
        KVClientServer.connect();
    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return KVClientServer;
    }

    private void handleCommand(String command) {
        String[] tokens = command.split("\\s+");
        String keyword = tokens[0];
        switch (keyword) {
            case "connect":
                if (tokens.length != 3) {
                    System.out.println("Connect failed: Invalid argument number");
                    logger.warn("Connect failed: Invalid argument number");
                } else {
                    try {
                        int port = Integer.parseInt(tokens[2]);
                        newConnection(tokens[1], port);
                        System.out.println("Connected to server");
                        logger.info("Connected to server");
                    } catch (IOException e) {
                        System.err.println("Establish connection failed: already connected");
                        logger.info("Establish connection failed: already connected");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
                break;
            case "disconnect":
                if (tokens.length != 1){
                    System.out.println("Disconnect failed: Invalid argument number");
                    logger.warn("Disconnect failed: Invalid argument number");
                } else {
                    if (KVClientServer != null) {
                        KVClientServer.disconnect();
                        KVClientServer = null;
                        System.out.println("Disconnected from server");
                        logger.info("Disconnected from server");
                    } else {
                        System.out.println("Disconnect failed: no connected server");
                        logger.warn("Disconnect failed: no connected server");
                    }
                }
                break;
            case "put":
                if (tokens.length != 3){
                    System.out.println("Put failed: Invalid argument number");
                    logger.warn("Put failed: Invalid argument number");
                } else {
                    try {
                        KVMessage message = KVClientServer.put(tokens[1], tokens[2]);
                    } catch (Exception e) {
                        System.out.println("Put failed: Server failed to insert the tuple.");
                        logger.error("Put failed: Server failed to insert the tuple.");
                    }


                }
                break;
            case "get":

                break;
            case "LogLevel":

                break;
            case "help":

                break;
            case "quit":
                running = false;
                if (KVClientServer != null) {
                    KVClientServer.disconnect();
                }
                break;
            default:

                break;
        }
    }


}
