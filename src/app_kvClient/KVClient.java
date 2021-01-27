package app_kvClient;

import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import client.KVCommInterface;
import shared.messages.KVMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class KVClient implements IKVClient, Runnable {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVService> ";
    private BufferedReader input;

    private KVStore KVClientServer;
    private boolean running;

    @Override
    public void run(){
        // TODO Auto-generated method stub
        running = true;
        while (running) {
            input = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);
            try {
                String cmdLine = input.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                running = false;
                System.out.println("CLI does not respond - Application terminated ");
            }
        }
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
                if (tokens.length != 3 && tokens.length != 2){
                    System.out.println("Put failed: Invalid arguments number");
                } else {
                    try {
                        KVMessage message;
                        if (tokens.length == 3) {
                            message = KVClientServer.put(tokens[1], tokens[2]);
                        } else {
                            message = KVClientServer.delete(tokens[1]);
                        }
                        handleServerResponse(message);
                    } catch (Exception e) {
                        e.printStackTrace();
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
                        e.printStackTrace();
                    }
                }
                break;
            case "logLevel":
                if (tokens.length != 2){
                    System.out.println("Set log level failed: Invalid arguments number");
                } else {
                    handleSetLevel(tokens[1]);
                }
                break;
            case "help":
                if (tokens.length != 1){
                    System.out.println("Help: Invalid arguments number");
                } else {
                    handleHelp();
                }
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
                handleHelp();
                break;
        }
    }

    private void handleServerResponse(KVMessage response){
        switch (response.getStatus()) {
            case GET_ERROR:
                System.out.println("Get failed: " + response.getKey() + " was invalid");
                logger.warn("Get failed: " + response.getKey() + " was invalid");
                break;
            case GET_SUCCESS:
                System.out.println(response.getValue());
                logger.info("Get succeed: (" + response.getKey() + ","
                        + response.getValue() + ")");
                break;
            case PUT_SUCCESS:
                System.out.println("Put succeed");
                logger.info("Put succeed: (" + response.getKey() + ","
                        + response.getValue() + ")");
                break;
            case PUT_UPDATE:
                System.out.println("Update succeed");
                logger.info("Update succeed: (" + response.getKey() + ","
                        + response.getValue() + ")");
                break;
            case PUT_ERROR:
                System.out.println("Put failed: (" + response.getKey()
                        + ", " + response.getValue() + ") was invalid");
                logger.warn("Put failed: (" + response.getKey()
                        + ", " + response.getValue() + ") was invalid");
                break;
            case DELETE_SUCCESS:
                System.out.println("Delete succeed");
                logger.info("Delete succeed: " + response.getKey());
                break;
            case DELETE_ERROR:
                System.out.println("Delete failed: " + response.getKey() + " was invalid");
                logger.warn("Delete failed: " + response.getKey() + " was invalid");
                break;
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
    }

    private void handleHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("Key-Value Service HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");
        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t Inserts a key-value pair into the storage server data structures.\n" +
                "Updates (overwrites) the current value with the given value if the " +
                "server already contains the specified key.\n" +
                "Deletes the entry for the given key if <value> equals null (\"\").\n");
        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.ALL);
            KVClient app = new KVClient();
            app.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }

}
