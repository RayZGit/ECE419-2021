package app_kvServer;

import org.apache.log4j.Logger;

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
        System.out.println("Receiver Data 8" );
    }

    @Override
    public synchronized void run() {
        System.out.println("Receiver Data 9" );
        try {
            socket = senderServerSocket.accept();
            System.out.println("Receiver Data 10" );
        } catch (IOException ex) {
            System.out.println("Can't accept sender server connection. ");
            logger.error("<Receive Server " + kvServer.getServerName() + "> Error! " + "Unable to establish connection. \n", ex);
        }

        try {
            inputStream = socket.getInputStream();
            System.out.println("Receiver Data 11" );
            System.out.println("Server Data Transfer Establishing connection !\n");
        } catch (IOException ex) {
            System.out.println("Can't get sender socket input stream. ");
        }

        try {
            outputStream = new FileOutputStream(kvServer.getServerDiskFile(), true);
            System.out.println("Receiver Data 12" );
        } catch (FileNotFoundException ex) {
            System.out.println("File not found. ");
        }

//        try {
//            length = inputStream.available();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        byte[] data = new byte[MAX_VALUE];
        int bytesRead = 0;
        int index = 0;
        //        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try{
            System.out.println("Transfer Data Start.");
            while ((bytesRead = inputStream.read()) != -1){
                System.out.println("");
                outputStream.write( data, 0, bytesRead );
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            outputStream.flush();
            outputStream.close();
            socket.close();
            senderServerSocket.close();
            kvServer.unlockWrite();
            System.out.println("Receiving Transfer Data Complete.");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
