package app_kvServer;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class KVServerDataTransferConnection implements Runnable {
    private static Logger logger = Logger.getRootLogger();
    private IKVServer kvServer;
    private boolean isOpen;
    private ServerSocket senderServerSocket;
    private static final int MAX_VALUE = 120 * 1024;
//    int length;

    public InputStream inputStream;
    public FileOutputStream outputStream;
    public Socket socket;

    public KVServerDataTransferConnection(ServerSocket senderServer, IKVServer kvServer) {
        this.senderServerSocket = senderServer;
        this.kvServer = kvServer;
        this.isOpen = true;
        System.out.println("KVServerDataTransferConnection 1" );
    }

    @Override
    public synchronized void run() {
        System.out.println("KVServerDataTransferConnection 2" );

        try {
            inputStream = socket.getInputStream();
            System.out.println("KVServerDataTransferConnection 3" );
            outputStream = new FileOutputStream(kvServer.getServerDiskFile(), true);
            System.out.println("Establishing connection !\n");
            logger.info("<Server> Establishing connection");

            while(isOpen) {
                try {
                    System.out.println("KVServerDataTransferConnection 4" );
                    byte[] data = new byte[MAX_VALUE];
                    int bytesRead = 0;
                    int index = 0;
                    //        ByteArrayOutputStream baos = new ByteArrayOutputStream();

                    System.out.println("Transfer Data Start.");
                    while ((bytesRead = inputStream.read()) != -1){
                        System.out.println("");
                        outputStream.write( data, 0, bytesRead );

                    }
                } catch (Exception e) {

                    System.out.println("Error! Connection lost!\n");
                    logger.error("<Server> Error! Connection lost <"
                            + this.socket.getInetAddress().getHostName()
                            +  ": " + this.socket.getPort() + ">");
                    isOpen = false;

                }

            }

        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);
        }
        finally {
            try {
                if (socket != null) {
                    inputStream.close();
                    outputStream.close();
                    socket.close();
                    kvServer.unlockWrite();
                    System.out.println("Receiving Transfer Data Complete.");
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }

    }
}
