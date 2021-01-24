package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.KVBasicMessage;
import shared.messages.KVMessage;

import java.io.*;
import java.net.Socket;

public class KVServerClientConnection implements Runnable {

    private static Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int MAX_KEY = 20;
    private static final int MAX_VALUE = 120 * 1024;

    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;

    public KVServerClientConnection(Socket client) {
        this.clientSocket = client;
        this.isOpen = true;
    }

    @Override
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            sendMessages(new KVBasicMessage(
                    "\"Connection to KEY-VALUE server established: "
                            + clientSocket.getLocalAddress() + " / "
                            + clientSocket.getLocalPort()));

            while(isOpen) {
                try {
                    KVMessage request = receiveMessage();
                    handleResquest(request);
                    /* connection either terminated by the client or lost due to
                     * network problems*/
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost!");
                    isOpen = false;
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }

        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);

        } finally {

            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    clientSocket.close();
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }


    /**
     * handle the request, <get, put>
     * @return true or false if the action success or failed
     */
    public boolean handleResquest(KVMessage request){
        //TODO
        return true;
    }


    public KVMessage receiveMessage() throws IOException, ClassNotFoundException {
        ObjectInputStream objectInputStream = new ObjectInputStream(input);
        KVMessage result = (KVMessage)objectInputStream.readObject();
        System.out.println("Receiving request from client");
        logger.info("RECEIVE \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: "
                + "key: " + result.getKey() +"|"
                + "value: " + result.getValue() + "|"
                + "status: " + result.getStatus() + "|"
                + "message: " + result.getMessages());
        return result;
    }

    public void sendMessages(KVMessage messages) throws IOException{
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(output);
        System.out.println("Sending response to the client");
        objectOutputStream.writeObject(messages);
        logger.info("SEND \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: "
                + "key: " + messages.getKey() +"|"
                + "value: " + messages.getValue() + "|"
                + "status: " + messages.getStatus() + "|"
                + "message: " + messages.getMessages());
    }
}
