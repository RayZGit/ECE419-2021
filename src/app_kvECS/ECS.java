package app_kvECS;


import ecs.ECSNode;
import ecs.HashRing;
import ecs.IECSNode;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import shared.messages.KVAdminMessage;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ECS {

    //Error Checking
    private static int NUMBER_ARGS_ECSCONFIG = 3; // number of args per line in ecs.config file
    private static String LOGGING = "ECS:";

    //Standalone Zookeeper Setting Config
    private static final String SERVER_JAR = "KVServer.jar";
    private static final String JAR_PATH = new File(System.getProperty("user.dir"), SERVER_JAR).toString();
    private static String ZK_HOST = "127.0.0.1";
    private static String ZK_PORT = "2181";
    private static int ZK_TIMEOUTSESSION = 3000;
    private static String ZNODE_ROOT = "/ZNode";

    private ZooKeeper zk;
    private Logger logger = Logger.getRootLogger();
    private Map<String, ECSNode> nodeMap;
    private HashRing hashRing;
    private Queue<IECSNode> nodeQueue;

    private class AdminDataHandler
    {
        private List<ECSNode> nodes;
        private CountDownLatch latch;
        private Map<String,String> errorMap;
        public Map<ECSNode,String> errorNodeMap;

        public AdminDataHandler(List<ECSNode> nodes){
            this.nodes = nodes;
            this.latch = new CountDownLatch(nodes.size());
            this.errorMap = new HashMap<String, String>();
            this.errorNodeMap = new HashMap<ECSNode,String>();
        }

        public boolean brodcast(KVAdminMessage msg) throws InterruptedException {
            for(ECSNode node: nodes)
            {
                String nodePath = ZNODE_ROOT+"/"+node.getNodeName();
                try {
                    Stat existNode = zk.exists(nodePath,false);
                    if(existNode == null){
                        zk.create(nodePath, msg.encode().getBytes(),
                                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } else{
                        zk.setData(nodePath,msg.encode().getBytes(), existNode.getVersion());
                    }
                    existNode = zk.exists(nodePath, new Watcher()
                    {
                        @Override
                        public void process(WatchedEvent event)
                        {
                            String error = null;
                            if (event.getType() == Event.EventType.NodeDataChanged){
                                // watch handled properly
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
            return wait;
        }

    }



    public ECS(String cfgFileName) throws IOException, InterruptedException {
        File cfg = new File(cfgFileName);
        String lines[] = cfg.list();

        for(String line : lines){
            String args[] = line.split(" ");
            if(args.length != NUMBER_ARGS_ECSCONFIG){
                logger.fatal(LOGGING+"The ECS object failed ");
            }
            nodeQueue.add(new ECSNode(args[0],args[1],Integer.parseInt(args[2])));
            logger.info(LOGGING+"New Node added: "+args[0]);
        }

        CountDownLatch latch = new CountDownLatch(1);
        zk = new ZooKeeper(ZK_HOST + ":" + ZK_PORT, ZK_TIMEOUTSESSION, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if(event.getType() == Event.EventType.None){
                    if(event.getState() == Event.KeeperState.SyncConnected){
                        latch.countDown();
                    }
                }else{
                    logger.error(LOGGING+"Zookeeper connected failed");
                    latch.countDown();
                }
            }
        });
        latch.await();
        metaDataHandler();


    }

    public boolean start() throws IOException, InterruptedException {

        //Step1: Looping through all the servers in the ecs.config
        //Step1.1: Check if they are in STOP status ->YES: they are about to be start
        //Step2: Setup the ZNode
        //STEP2: add them to the hashingRing
        //STEP3: Notify and get response
        List<ECSNode> startNode = new ArrayList<>();
        for(String serverName : nodeMap.keySet())
        {
            ECSNode node = nodeMap.get(serverName);
            if(true){ // stub-code // TODO: ADD THE NODE STATUS TO TRACK IF ITS STOP
                startNode.add(node);
                hashRing.addNode(node);
            }
        }
        AdminDataHandler adminDataHandler = new AdminDataHandler(startNode);
        boolean flag = adminDataHandler.brodcast(new KVAdminMessage(KVAdminMessage.ServerFunctionalType.START));

        for(ECSNode node : startNode){
            if(adminDataHandler.errorNodeMap.containsKey(node)){
                hashRing.removeNode(node);
                continue;
            }
//            n.set // TODO: SET STATUS OF NODE TO ACTIVE
        }
        metaDataHandler();
        return flag;

    }

    public boolean stop() throws InterruptedException {
        List<ECSNode> stopNode = new ArrayList<>();
        for(String node: nodeMap.keySet()){
            if(true) {// TODO: check if its active
                stopNode.add(nodeMap.get(node));
            }
        }

        AdminDataHandler adminDataHandler = new AdminDataHandler(stopNode);
        boolean flag = adminDataHandler.brodcast(new KVAdminMessage(KVAdminMessage.ServerFunctionalType.STOP));

        for(ECSNode node : stopNode){
            if(adminDataHandler.errorNodeMap.containsKey(node)){
                hashRing.removeNode(node);
                continue;
            }
//            n.set // TODO: SET STATUS OF NODE TO ACTIVE
        }
        metaDataHandler();
        return flag;

    }

    public boolean shutdown() throws InterruptedException {
        List<ECSNode> shutdownNode = new ArrayList<>();
        for(String node: nodeMap.keySet()){
            shutdownNode.add(nodeMap.get(node));
        }
        AdminDataHandler adminDataHandler = new AdminDataHandler(shutdownNode);
        boolean flag = adminDataHandler.brodcast(new KVAdminMessage(KVAdminMessage.ServerFunctionalType.SHUT_DOWN));

        if(flag){
            nodeMap.clear();
//            hashRing.removeNode();// TODO: REMOVE ALL THE NODE
        }
        return flag;
    }

    public IECSNode addNode(String cacheStrategy, int cacheSize){
        Collection<IECSNode> node = addNodes(1, cacheStrategy, cacheSize);
        if(node == null) return null;
        return (IECSNode) node.toArray()[0];
    }

    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize){
        Collection<IECSNode> nodes = setupNodes(count, cacheStrategy, cacheSize);
        if(nodes == null) return  null;
        for(IECSNode node: nodes)
        {
            String javaCmd = String.join(" ",
                    "java -jar",
                    JAR_PATH,
                    String.valueOf(node.getNodePort()),
                    node.getNodeName(),
                    ZK_HOST,
                    ZK_PORT);
            String sshCmd = "ssh -o StrictHostKeyChecking=no -n " + node.getNodeHost() + " nohup " + javaCmd + " &";
            logger.info("Executing command: " + sshCmd);
            try {
                Process p = Runtime.getRuntime().exec(sshCmd);
                p.waitFor();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                nodes.remove(node);
            }
        }
        return null;

    }
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize){
        if(count > nodeQueue.size()) return null;

        List<IECSNode> ret = new ArrayList<>();
        while(!nodeQueue.isEmpty()&&count>0){
            ret.add(nodeQueue.poll());
            count--;
        }
        byte[] metaData = null; // TODO: STUB FOR METADATA

        metaDataHandler();
        //TODO: SETUP FOR ZNODE
        return ret;
    }

    public boolean awaitNodes(int count, int timeout) throws Exception{
        List<ECSNode> waitNode = new ArrayList<>();
        for(String name: nodeMap.keySet()){
            ECSNode node = nodeMap.get(name);
//            if(node.ge) // TODO:check the status of the node
            if(true){
                waitNode.add(node);
            }
        }

        AdminDataHandler adminDataHandler = new AdminDataHandler(waitNode);
        boolean flag = adminDataHandler.brodcast(new KVAdminMessage(KVAdminMessage.ServerFunctionalType.INIT_KV_SERVER));

        //TODO:SET THE STATUS OF EACH NODE
        return flag;

    }


    private void setUpZNode(String path, byte[] msg) throws Exception{
        Stat nodeStatus = zk.exists(path, false);
        if(nodeStatus == null){
            zk.create(path, msg, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }else{
            zk.setData(path,msg,nodeStatus.getVersion());
        }
    }
    public Map<String, ECSNode> getNodes(){
        return (Map<String, ECSNode>) nodeMap;
    }
    public IECSNode getNodeByKey(String Key){
        return (IECSNode) nodeMap.get(Key);
    }




    private void metaDataHandler(){
        byte[] dataForHashPosition = null; // stub-code //TODO: PLZ REPLACE WITH REAL METADATA IN Hashring
        try {
            Stat existMD = zk.exists("/MD", false);
            if (existMD == null){
                zk.create("/MD",dataForHashPosition,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            else{
                zk.setData("/MD",dataForHashPosition, existMD.getVersion());
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}






