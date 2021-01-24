package app_kvClient;

import client.KVStore;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import client.KVCommInterface;
import shared.messages.KVMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.UnknownHostException;

public class KVClient implements IKVClient, Runnable {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "EchoClient> ";
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

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");
        String cmd = tokens[0];
        switch (cmd) {
            case "connect":
                if (tokens.length != 3) {
                    System.out.println("Connect failed: Invalid argument number");
                } else {
                    try {
                        int port = Integer.parseInt(tokens[2]);
                        newConnection(tokens[1], port);
                        System.out.println("Connected to server");
                        logger.info("Connected to server");
                    } catch (Exception e) {
                        System.out.println("Establish connection failed");
                        logger.warn("Establish connection failed");
                    }
                }
                break;
            case "disconnect":
                if (tokens.length != 1){
                    System.out.println("Disconnect failed: Invalid arguments number");
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
                    System.out.println("Put failed: Invalid arguments number");
                } else {
                    try {
                        KVClientServer.put(tokens[1], tokens[2]);
                        handleServerResponse(KVClientServer.receiveMessage());
                    } catch (Exception e) {
                        System.out.println("Put failed: Server failed to insert the tuple.");
                        logger.error("Put failed: Server failed to insert the tuple.");
                    }
                }
                break;
            case "get":
                if (tokens.length != 2){
                    System.out.println("Get failed: Invalid arguments number");
                } else {
                    try {
                        KVMessage message = KVClientServer.get(tokens[1]);
                        handleServerResponse(message);
                    } catch (Exception e) {
                        System.out.println("Get failed: Server failed to get the tuple.");
                        logger.error("Get failed: Server failed to get the tuple.");
                    }
                }
                break;
            case "LogLevel":
                if (tokens.length != 2){
                    System.out.println("Set log level failed: Invalid arguments number");
                } else {
                    handleSetLevel(tokens[1]);
                }
                break;
            case "help":
                handleHelp();
                break;
            case "quit":
                running = false;
                if (KVClientServer != null) {
                    KVClientServer.disconnect();
                }
                System.out.println("Disconnected");
                logger.info("Disconnected");
                break;
            default:
                System.out.println("Unknown command");
                break;
        }
    }

    private void handleServerResponse(KVMessage response){
        switch (response.getStatus()) {
            case GET_ERROR -> {
                System.out.println("Get failed: " + response.getKey() + " was invalid");
                logger.warn("Get failed: " + response.getKey() + " was invalid");
            }
            case GET_SUCCESS -> {
                System.out.println(response.getValue());
                logger.info("Get succeed: (" + response.getKey() + ","
                        + response.getValue() + ")");
            }
            case PUT_SUCCESS -> {
                System.out.println("Put succeed");
                logger.info("Put succeed" + response.getKey() + ","
                        + response.getValue() + ")");
            }
            case PUT_UPDATE -> {
                System.out.println("Update succeed");
                logger.info("Update succeed" + response.getKey() + ","
                        + response.getValue() + ")");
            }
            case PUT_ERROR -> {
                System.out.println("Put failed: (" + response.getKey()
                        + ", " + response.getValue() + ") was invalid");
                logger.warn("Put failed: (" + response.getKey()
                        + ", " + response.getValue() + ") was invalid");
            }
            case DELETE_SUCCESS -> {
                System.out.println("Delete succeed");
                logger.info("Delete succeed: " + response.getKey());
            }
            case DELETE_ERROR -> {
                System.out.println("Delete failed: " + response.getKey() + " was invalid");
                logger.warn("Delete failed: " + response.getKey() + " was invalid");
            }
        }
    }

    private void handleSetLevel(String level) {
        if (level.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
        } else if (level.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
        } else if (level.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
        } else if (level.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
        } else if (level.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
        } else if (level.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
        } else if (level.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
        } else {
            System.out.println("Set log level failed: Invalid level.");
            logger.warn("Set log level failed: Invalid level.");
            return;
        }
        System.out.println("Set log level to " + level);
        logger.info("Set log level to " + level);
    }

    private void handleHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("send <text message>");
        sb.append("\t\t sends a text message to the server \n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }


}
