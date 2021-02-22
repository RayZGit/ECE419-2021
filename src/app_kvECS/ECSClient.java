package app_kvECS;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import ecs.ECSNode;
import ecs.HashRing;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import server.ServerMetaData;
import shared.messages.KVAdminMessage;

public class ECSClient implements IECSClient {

    //Error Checking
    private static int NUMBER_ARGS_ECSCONFIG = 3; // number of args per line in ecs.config file
    private static String LOGGING = "ECSClient: ";

    //Standalone Zookeeper Setting Config
    private static final String SERVER_JAR = "KVServer.jar";
    private static final String JAR_PATH = new File(System.getProperty("user.dir"), SERVER_JAR).toString();
    private static String ZK_HOST = "127.0.0.1";
    private static String ZK_PORT = "2181";
    private static int ZK_TIMEOUTSESSION = 3000;
    private static String ZNODE_ROOT = "/ZNode";
    private static String METADATA_ROOT = "/MD";
    private static String ZNODE_KVMESSAGE = "/KVAdminMessage";

    private Logger logger = Logger.getRootLogger();
    private String script = "script.sh";

    private ZooKeeper zk; // ZooKeeper manager
    private Map<String, IECSNode> nodeMap; // Map for all the nodes added
    private HashRing hashRing; // The hashing ring of all the node in active status
    private Queue<IECSNode> nodeQueue; // The nodes pool


    private class AdminDataHandler
    {
        public Map<ECSNode,String> brodcast(byte[] msg, List<ECSNode> nodes, boolean KVadmin) throws InterruptedException{
//            msg.encode().getBytes()
            CountDownLatch latch = new CountDownLatch(nodes.size());
            HashMap<String, String> errorMap = new HashMap<String, String>();
            Map<ECSNode,String> errorNodeMap = new HashMap<ECSNode,String>();
            for(ECSNode node: nodes)
            {
                String nodePath = ZNODE_ROOT+"/"+node.getNodeName(); // /ZNode/ServerName
                if(KVadmin) nodePath += "/" + ZNODE_KVMESSAGE;// /ZNode/ServerName/KVAdminMessage
                try {
                    Stat existNode = zk.exists(nodePath,false);
                    if(existNode == null){
                        zk.create(nodePath, msg,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } else{
                        zk.setData(nodePath,msg, existNode.getVersion());
                    }
                    existNode = zk.exists(nodePath, new Watcher()
                    {
                        @Override
                        public void process(WatchedEvent event)
                        {
                            String error = null;
                            latch.countDown();
                            if (event.getType() == Event.EventType.NodeDataChanged){
                                // watch handled properly
                                //expected the server to set back to null
                            }
                            else{
                                error = "Unexpected Error" + event.getType();
                            }
                            if(error != null){
                                errorMap.put(event.getPath(), error);
                            }

                        }
                    });

                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            boolean wait = latch.await(ZK_TIMEOUTSESSION, TimeUnit.MILLISECONDS);
            for(ECSNode node : nodes){
                String nodePath = ZNODE_ROOT+"/"+node.getNodeName();
                if(errorMap.containsKey(nodePath)){
                    errorNodeMap.put(node,errorNodeMap.get(nodePath));
                }
            }
            for (String nodePath : errorMap.keySet()){
                logger.error(LOGGING+errorMap.get(nodePath));
            }
            return errorNodeMap;
        }
    }

    private AdminDataHandler adminDataHandler;

    public ECSClient (String cfgFileName) throws IOException, InterruptedException {
        adminDataHandler = new AdminDataHandler();
        hashRing = new HashRing();
        nodeMap = new HashMap<>();

        File cfg = new File(cfgFileName);
        String lines[] = cfg.list();

        //Get all the nodes from config file into pool
        for(String line : lines){
            String args[] = line.split(" ");
            if(args.length != NUMBER_ARGS_ECSCONFIG){
                logger.fatal(LOGGING+"The ECS object failed ");
            }
            nodeQueue.add(new ECSNode(args[0],args[1],Integer.parseInt(args[2])));
            logger.info(LOGGING+"New Node added: "+args[0]);
        }

        //Setup the zk (ZooKeeper)
        CountDownLatch latch = new CountDownLatch(1);
        zk = new ZooKeeper(ZK_HOST + ":" + ZK_PORT, ZK_TIMEOUTSESSION, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if(event.getState() == Event.KeeperState.SyncConnected){
                        latch.countDown();
                    }
                else{
                    logger.error(LOGGING+"Zookeeper connected failed");
                    latch.countDown();
                }
            }
        });
        latch.await();
        //In the initialization, the metaDataHandler should only creat an node called /MD
        metaDataHandler();

    }

    @Override
    public boolean start() throws InterruptedException {
        List<ECSNode> startNode = new ArrayList<>();
        for(String serverName : nodeMap.keySet())
        {
            ECSNode node = (ECSNode)nodeMap.get(serverName);
            if(node.getStatus().equals(ECSNode.NodeStatus.STOP)){
                startNode.add(node);
                hashRing.addNode(node);
            }
        }

        addNodeHandler(startNode);

        KVAdminMessage msg = new KVAdminMessage(KVAdminMessage.ServerFunctionalType.START);
        Map<ECSNode,String> nodeErrorMap  = adminDataHandler.brodcast(msg.encode().getBytes(),startNode, true);
        for(ECSNode node : startNode){
            if(nodeErrorMap.containsKey(node)){
                hashRing.removeNode(node);
                continue;
            }
            node.setStatus(ECSNode.NodeStatus.START);
        }
        metaDataHandler();
        return nodeErrorMap.isEmpty();
    }

    @Override
    public boolean stop() throws InterruptedException {
        List<ECSNode> stopNode = new ArrayList<>();
        for(String nodeName: nodeMap.keySet()){
            ECSNode node = (ECSNode)nodeMap.get(nodeName);
            if(node.getStatus().equals(ECSNode.NodeStatus.START)) {
                stopNode.add(node);
            }
        }
        KVAdminMessage msg = new KVAdminMessage(KVAdminMessage.ServerFunctionalType.STOP);
        Map<ECSNode,String> nodeErrorMap  = adminDataHandler.brodcast(msg.encode().getBytes(),stopNode, true);

        for(ECSNode node : stopNode){
            if(nodeErrorMap.containsKey(node)){
                hashRing.removeNode(node);
                continue;
            }
            node.setStatus(ECSNode.NodeStatus.STOP);
        }
        metaDataHandler();
        return nodeErrorMap.isEmpty();
    }

    @Override
    public boolean shutdown() throws InterruptedException {
        List<ECSNode> shutDownNode = new ArrayList<>();
        for(String nodeName: nodeMap.keySet()){
            ECSNode node = (ECSNode)nodeMap.get(nodeName);
            shutDownNode.add(node);
        }

        KVAdminMessage msg = new KVAdminMessage(KVAdminMessage.ServerFunctionalType.SHUT_DOWN);
        Map<ECSNode,String> nodeErrorMap  = adminDataHandler.brodcast(msg.encode().getBytes(),shutDownNode, true);
        if(nodeErrorMap.isEmpty()){
            hashRing.removeAll();
            for(String nodeName: nodeMap.keySet()){
                ECSNode node = (ECSNode)nodeMap.get(nodeName);
                node.setStatus(ECSNode.NodeStatus.IDLE);
                nodeQueue.add(node);
            }
            nodeMap.clear();
            metaDataHandler();
        }
        return nodeErrorMap.isEmpty();
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        Collection<IECSNode> nodes = addNodes(1,cacheStrategy,cacheSize);
        if(nodes == null) return null;
        return (IECSNode)nodes.toArray()[0];
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        Collection<IECSNode> nodes = setupNodes(count, cacheStrategy, cacheSize);
        if(nodes == null) return null;
        for (IECSNode node: nodes){
            boolean flag = sshServer((ECSNode) node);
            if(!flag){
                nodes.remove(node);
            }
        }
        boolean flag = false;
        try {
            flag = awaitNodes(count,ZK_TIMEOUTSESSION);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(LOGGING+"addNodes after await");
            return null;
        }
        return flag? nodes:null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        if(count > nodeQueue.size()){
            logger.error(LOGGING+"The error happened during the SetupNodes and count is greater than available nodes");
            return null;
        }
        List<IECSNode> nodes = new ArrayList<>();
        while(!nodeQueue.isEmpty() && count > 0){
            nodes.add((IECSNode) nodeQueue.poll());
            count--;
        }
        for(IECSNode node: nodes){
            byte[] metadata = new Gson().toJson(new ServerMetaData(cacheSize,cacheStrategy,
                    node.getNodePort(), node.getNodeHost() )).getBytes();
            try {
                adminDataHandler.brodcast(metadata,new ArrayList<ECSNode>(Arrays. asList((ECSNode) node)), false);
                // delete all the sub node;
                String path = ZNODE_ROOT+"/"+node.getNodeName();
                List<String> children = zk.getChildren(path, false);
                for (String zn : children) {
                    String subPath = path + "/" + zn;
                    Stat ex = zk.exists(subPath, false);
                    zk.delete(subPath, ex.getVersion());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error(LOGGING+"setupNodes error");
            } catch (KeeperException e) {
                e.printStackTrace();
            }


        }
        for (IECSNode node : nodes) {
            ((ECSNode) node).setStatus(ECSNode.NodeStatus.WAIT);
            nodeMap.put(node.getNodeName(), node);
        }
        return nodes;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        KVAdminMessage msg = new KVAdminMessage(KVAdminMessage.ServerFunctionalType.INIT_KV_SERVER);
        List<ECSNode> nodes = new ArrayList<>();
        for(String nodeName : nodeMap.keySet()){
            ECSNode node = (ECSNode) nodeMap.get(nodeName);
            if(node.getStatus().equals(ECSNode.NodeStatus.WAIT)){
                nodes.add(node);
            }
        }
        Map<ECSNode,String> errorNodeMap = adminDataHandler.brodcast(msg.encode().getBytes(),nodes,true);
        if(errorNodeMap.isEmpty()){
            for(ECSNode n : nodes){
                n.setStatus(ECSNode.NodeStatus.STOP);
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        List<ECSNode> removeNodes = new ArrayList<>();
        List<ECSNode> activeNodes = new ArrayList<>();
        for(String name:nodeNames){
            if(nodeMap.containsKey(name)){
                ECSNode node = (ECSNode) nodeMap.get(name);
                removeNodes.add(node);
                if(node.getStatus().equals(ECSNode.NodeStatus.START))
                    activeNodes.add(node);
            }
        }

        removeNodeHandler(activeNodes);

        KVAdminMessage msg = new KVAdminMessage(KVAdminMessage.ServerFunctionalType.SHUT_DOWN);
        try {
            adminDataHandler.brodcast(msg.encode().getBytes(), removeNodes,true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error(LOGGING+"removeNode error");
            return false;
        }

        for(ECSNode node : removeNodes){
            hashRing.removeNode(node);
            nodeMap.remove(node.getNodeName());
            node.setStatus(ECSNode.NodeStatus.IDLE);
            nodeQueue.add(node);
        }

        metaDataHandler();
        return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        return nodeMap;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return nodeMap.getOrDefault(Key, null);
    }

    //==============================================Helper Functions====================================================

    private void metaDataHandler(){
        // Extract the Metadata from Hashring (it contains all the nodes in active)
        List<ECSNode> activeNods = new ArrayList<>();
        for(String nodeName: nodeMap.keySet()){
            ECSNode node = (ECSNode)nodeMap.get(nodeName);
            if(node.getStatus().equals(ECSNode.NodeStatus.START)) {
                activeNods.add(node);
            }
        }

        byte[] dataForHashPosition = new Gson().toJson(activeNods).getBytes();
        try {
            Stat existMD = zk.exists(METADATA_ROOT, false);
            if (existMD == null){
                zk.create(METADATA_ROOT,dataForHashPosition, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            else{
                zk.setData(METADATA_ROOT,dataForHashPosition, existMD.getVersion());
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private boolean sshServer(ECSNode node){
        //TODO: CHECK LATER FOLLOW KVSERVER
        String hostname = node.getNodeHost();
        String portnumber = String.valueOf(node.getNodePort());
        String cmd = String.format("ssh -n %s nohup java -jar %s %s ERROR &", hostname, JAR_PATH, portnumber);
        try {
            Process proc = Runtime.getRuntime().exec(cmd);
            int exitCode = proc.waitFor();
            assert exitCode == 0;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            logger.error(LOGGING+"sshSever error");
            return false;
        }
        return true;

    }

    private void removeNodeHandler(List<ECSNode> nodes){
        for (ECSNode node : nodes) {
            ECSNode dest = locateNode(node, nodes);
            if(dest == null){
                logger.warn("No server available to accept data from the deletion of node " + node.getNodeName());
                logger.warn("The node is the last node active, service down...");
            }
            else{
                logger.info("next node found-------starting transfering data from"+ node.getNodeName() +
                        " to "+dest.getNodeName());
                dataTransition(node,dest,node.getNodeHashRange());
            }
        }
    }
    private void addNodeHandler(List<ECSNode> nodes){
        for (ECSNode node : nodes) {
            ECSNode start = locateNode(node, nodes);
            if(start == null){
                logger.warn("No server exist yet. No data to transfer to node " + node.getNodeName());
            }
            else{
                logger.info("next node found-------starting transfering data from"+ start.getNodeName() +
                        " to "+node.getNodeName());
                dataTransition(start,node,node.getNodeHashRange());
            }
        }
    }

    private ECSNode locateNode(ECSNode node, List<ECSNode> reference){
        ECSNode rec = node;
        int count = 0;
        while(true){
            count ++;
            //TODO: IMPORTANT GET NEXT NODE ON HASHRING
            if(!reference.contains(rec)){
                return rec;
            }
            if(rec.equals(node)) return null;
            if(count>hashRing.getSize()){
                logger.error(LOGGING+"locateNode error");
                break;
            }
        }
        return null;

    }

    public class Transfer{
        private boolean fromDone;
        private boolean toDone;
        private String fromPath;
        private String toPath;
        private CountDownLatch latch = null;
        private Watcher watchFrom = null;
        private Watcher watchTo = null;

        public Transfer(ECSNode from, ECSNode to){
            this.fromPath = ZNODE_ROOT + "/" + from.getNodeName();
            this.toPath = ZNODE_ROOT + "/" + to.getNodeName();
            this.fromDone = false;
            this.toDone = false;
            this.watchFrom = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDataChanged){
                        checkFrom();
                    }

                }
            };
            watchTo = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDataChanged){
                        checkTo();
                    }

                }
            };
        }
        private void checkFrom(){
            try {
                byte[] serverData = zk.getData(fromPath,false, null);
                ServerMetaData data = new Gson().fromJson(serverData.toString(), ServerMetaData.class);
                if(data.equals(ServerMetaData.ServerDataTransferProgressStatus.IDLE)){
                    this.fromDone = true;
                    latch.countDown();
                }
                else{
                    zk.exists(this.fromPath,this.watchFrom);
                }

            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        private void checkTo(){
            try {
                byte[] serverData = zk.getData(toPath,false, null);
                ServerMetaData data = new Gson().fromJson(serverData.toString(), ServerMetaData.class);
                if(data.equals(ServerMetaData.ServerDataTransferProgressStatus.IDLE)){
                    this.toDone = true;
                    latch.countDown();
                }
                else{
                    zk.exists(this.toPath,this.watchTo);
                }

            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        public boolean check(){
            this.latch = new CountDownLatch(2);
            try {
                checkFrom();
                checkTo();
                boolean wait = latch.await(ZK_TIMEOUTSESSION, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error(LOGGING+"INNER CLASS: "+ "Transfer");
                return false;
            }

            return this.toDone && this.fromDone;
        }
    }




    private boolean dataTransition(ECSNode from, ECSNode to, String[] range){
        try {
            KVAdminMessage msgTo = new KVAdminMessage(KVAdminMessage.ServerFunctionalType.RECEIVE);
            KVAdminMessage msgFrom = new KVAdminMessage(KVAdminMessage.ServerFunctionalType.MOVE_DATA);
            msgFrom.setReceiverHost(to.getNodeHost());
            msgFrom.setReceiverName(to.getNodeName());
            msgFrom.setReceiveHashRangeValue(range);
            Map<ECSNode,String> nodeErrorMapRec = adminDataHandler.
                    brodcast(msgTo.encode().getBytes(),new ArrayList<ECSNode>(Arrays.asList(to)),true);
            if(!nodeErrorMapRec.isEmpty()){
                logger.error(LOGGING+"dataTransition in Receive");
                return false;
            }

            Map<ECSNode,String> nodeErrorMapMov = adminDataHandler.
                    brodcast(msgFrom.encode().getBytes(),new ArrayList<ECSNode>(Arrays.asList(from)), true);

            if(!nodeErrorMapRec.isEmpty()){
                logger.error(LOGGING+"dataTransition in Send");
                return false;
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }

        Transfer ack = new Transfer(from, to);
        return ack.check();

    }

    //===================================================Start Functions================================================

    public static void main(String[] args) {

        try {
            new LogSetup("logs/ecsclient.log", Level.ALL);
            if (args.length < 1) {
                System.err.println("Error! Invalid number of arguments!");
                System.err.println("Usage: ECS <config file>!");
            } else {
                ECSClient ecs = new ECSClient(args[0]);
                BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
                boolean running = true;
                while(running){
                    String cmd = stdin.readLine();
                    String[] tokens = cmd.split("\\s+");
                    assert tokens.length > 0;
                    boolean result = false;
                    switch (tokens[0]){
                        case "start":
                            result = ecs.start();
                            break;
                        case "stop":
                            result = ecs.stop();
                            break;
                        case "shutDown":
                            result = ecs.shutdown();
                            break;
                        case "addNode":
                            try {
                                IECSNode node = ecs.addNode(
                                        tokens[1],
                                        Integer.parseInt(tokens[2])
                                );
                                result = (node != null);
                            } catch (NumberFormatException nfe) {
                                System.out.println("Unable to parse argument <cacheSize>"+nfe.getMessage());
                            } catch (IllegalArgumentException iae) {
                                System.out.println("Error! Invalid <strategy>! Must be one of [None LRU LFU FIFO]!");
                            }
                            break;
                        case "removeNodes":
                            List<String> serverNames = new ArrayList<>(Arrays.asList(tokens));
                            serverNames.remove(0); // remove cmd from head
                            result = ecs.removeNodes(serverNames);
                            break;
                        case "quit":
                            running = false;
                            result = true;
                            break;
                        default:
                            System.out.println("Unknown command!");
                    }
                    if (result) {
                        System.out.println("operation successful");
                    } else {
                        System.out.println( "operation failed");
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Application termination------"+e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("Application termination------"+e.getMessage());
        }
    }
}

