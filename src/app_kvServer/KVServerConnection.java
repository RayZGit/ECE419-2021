package app_kvServer;

import ecs.ECSNode;
import ecs.HashRing;
import org.apache.log4j.Logger;
import shared.KVMsgProtocol;
import shared.messages.KVBasicMessage;
import shared.messages.KVMessage;

import java.io.*;
import java.net.Socket;

public class KVServerConnection extends KVMsgProtocol implements Runnable {

    private static Logger logger = Logger.getRootLogger();
    private IKVServer kvServer;
    private boolean isOpen;
    private static final int MAX_KEY = 20;
    private static final int MAX_VALUE = 120 * 1024;

    private Socket clientSocket;

    public KVServerConnection(Socket client, IKVServer kvServer) {
        this.clientSocket = client;
        this.isOpen = true;
        this.kvServer = kvServer;
    }

    @Override
    public void run() {
        try {
            outputStream = clientSocket.getOutputStream();
            inputStream = clientSocket.getInputStream();
            System.out.println("Establishing connection !\n");
            logger.info("<Server> Establishing connection");

            while(isOpen) {
                try {
                    KVMessage request = receiveMsg();
//                    System.out.println("!!!!!!!!!!!!Received before handle client request!!!!!!!!!!!!!!");
                    KVMessage response = handleClientRequest(request);
                    sendMsg(response);
                    /* connection either terminated by the client or lost due to
                     * network problems*/
                } catch (Exception e) {
                    System.out.println("<Server> Write cache data to disk!");
                    logger.info("<Server>  Write cache data to disk!");

                    System.out.println("Cache size is: " + kvServer.getCacheSize());
                    kvServer.writeCacheToDisk();
//                    kvServer.clearCache();
                    System.out.println("Cache size is: " + kvServer.getCacheSize());

                    System.out.println("Error! Connection lost!\n");
                    logger.error("<Server> Error! Connection lost <"
                            + this.clientSocket.getInetAddress().getHostName()
                            +  ": " + this.clientSocket.getPort() + ">");
                    isOpen = false;
                }

            }

        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);

        } finally {
            try {
                if (clientSocket != null) {
                    inputStream.close();
                    outputStream.close();
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
    public KVMessage handleClientRequest(KVMessage request){
        KVMessage response = new KVBasicMessage();
        response.setKey(request.getKey());

        if (kvServer.isDistributed()) {
            HashRing hashRing = new HashRing(kvServer.getServerHashRings());
            if (kvServer.getServerStatus().equals(IKVServer.ServerStatus.STOP) || hashRing == null){
                response.setValue("");
                response.setStatus(KVMessage.StatusType.SERVER_STOPPED);
            }

            ECSNode node = hashRing.getNodeByKey(request.getKey());
            if (node == null) {
                System.out.println("Error! Can not find Corresponding Node with hashRing: <" + hashRing + ">");
                logger.error("Error! Can not find Corresponding Node with hashRing: <" + hashRing + ">");
            }

            boolean isResponsible = node.getNodeName().equals(kvServer.getServerName());
            if (isResponsible == false) {
                response.setValue(kvServer.getServerHashRings());
                response.setStatus(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
                return response;
            }

        }


        switch (request.getStatus()) {
            case GET: {
                Exception exception = null;
                String value = null;
                try {
                    value = kvServer.getKV(request.getKey());
                } catch (Exception e) {
                    exception = e;
                }

                if (exception != null || value == null) {
                    response.setValue("");
                    response.setStatus(KVMessage.StatusType.GET_ERROR);
                    System.out.println("Get failed");
                    logger.info("<Server> Get failed: ( key," + response.getKey() + " )");

                } else {
                    response.setValue(value);
                    response.setStatus(KVMessage.StatusType.GET_SUCCESS);

                    System.out.println("Get succeed");
                    logger.info("<Server> Get succeed" + response.getKey() + ","
                            + value + ")");
                }
                break;
            }

            case DELETE: {
                if (kvServer.getServerStatus().equals(IKVServer.ServerStatus.LOCK)) {
                    response.setValue(kvServer.getServerHashRings());
                    response.setStatus(KVMessage.StatusType.SERVER_WRITE_LOCK);
                    return response;
                }
                response.setValue(request.getValue());
                boolean keyExist = kvServer.inCache(request.getKey()) || kvServer.inStorage(request.getKey());
//                System.out.println("Delete fk!!!!!!!!!!!!!!!!!!!!!!!!!");
                System.out.println("keyExist is " + keyExist);
                if (keyExist) {
                    try {
                        kvServer.delete(request.getKey());
                        response.setStatus(KVMessage.StatusType.DELETE_SUCCESS);
                        System.out.println("Delete succeed");
                        logger.info("<Server> Delete succeed. Key: (" + request.getKey() + ")");
                    } catch (Exception e) {
                        response.setStatus(KVMessage.StatusType.DELETE_ERROR);
                        System.out.println("Delete failed: (" + request.getKey()
                                + ", " + request.getValue() + ") was invalid");
                        logger.error("<Server> Delete failed: (" + request.getKey()
                                + ", " + request.getValue() + ") was invalid");
                        break;
                    }
                    break;
                } else{
                    response.setStatus(KVMessage.StatusType.DELETE_ERROR);
                    System.out.println("Delete failed: Key (" + request.getKey()  + ") does not exist,");
                    logger.error("Delete failed: Key(" + request.getKey()  + ") does not exist,");
                }
                break;
            }

            case PUT: {
//                System.out.println("In put 1");
                if (kvServer.getServerStatus().equals(IKVServer.ServerStatus.LOCK)) {
                    response.setValue(kvServer.getServerHashRings());
                    response.setStatus(KVMessage.StatusType.SERVER_WRITE_LOCK);
                    return response;
                }
                response.setValue(request.getValue());
                String requestKey = request.getKey();
                String requestValue = request.getValue();

                if (requestKey.equals("") || requestKey.getBytes().length > MAX_KEY ||
                        requestValue.equals("") || requestValue.getBytes().length > MAX_VALUE) {

                    response.setStatus(KVMessage.StatusType.PUT_ERROR);
                    System.out.println("Put failed: (" + requestKey
                            + ", " + requestValue + ") was invalid");
                    logger.error("<Server> Put failed: (" + requestKey
                            + ", " + requestValue + ") was invalid");
                    break;
                }

//                System.out.println("In put 2");
                boolean keyExist = kvServer.inCache(requestKey) || kvServer.inStorage(requestKey);
                try {
                    kvServer.putKV(requestKey, requestValue);
                } catch (Exception e) {
                    response.setStatus(KVMessage.StatusType.PUT_ERROR);
                    System.out.println("Put failed: (" + requestKey
                            + ", " + requestValue + ") was invalid");
                    logger.error("<Server> Put failed: (" + requestKey
                            + ", " + requestValue + ") was invalid");
                    break;
                }

                if (keyExist) {
                    response.setStatus(KVMessage.StatusType.PUT_UPDATE);
                    System.out.println("Update succeed");
                    logger.info("<Server> Update succeed" + requestKey + ","
                            + requestValue + ")");
                } else {
                    response.setStatus(KVMessage.StatusType.PUT_SUCCESS);
                    System.out.println("Put succeed");
                    logger.info("<Server> Put succeed" + requestKey + ","
                            + requestValue + ")");
                }
                break;
            }
        }

        if (kvServer.getCache() != null) {
            System.out.println("Current cache has node#: " + kvServer.getCacheSize() + " ||| " + kvServer.getCache());
        }

        return  response;
    }


    public KVMessage receiveMsg() throws Exception {
        KVMessage request = receiveMessage();
        logger.info("<Server> RECEIVE from \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: "
                + "key: " + request.getKey() +" | "
                + "value: " + request.getValue() + " | "
                + "status: " + request.getStatus());
        System.out.println("<Server> RECEIVE from \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: "
                + "key: " + request.getKey() +" | "
                + "value: " + request.getValue() + " | "
                + "status: " + request.getStatus());
        return request;
    }

    public void sendMsg(KVMessage messages) throws Exception{
        sendMessage(messages);
        logger.info("<Server> SEND \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: "
                + "key: " + messages.getKey() +"| "
                + "value: " + messages.getValue() + "| "
                + "status: " + messages.getStatus());
        System.out.println("<Server> SEND \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: "
                + "key: " + messages.getKey() +"| "
                + "value: " + messages.getValue() + "| "
                + "status: " + messages.getStatus());
    }
}
