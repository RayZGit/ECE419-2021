package ecs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class HashRing {
    private static Logger logger = Logger.getRootLogger();
    private static MessageDigest md;
    private ECSNode first;

    public int getSize() {
        return size;
    }

    private int size;

    static {
        try {
            md = MessageDigest.getInstance("MD5");
            md.reset();
        } catch (NoSuchAlgorithmException e) {
            logger.fatal("Hash function does not exist!");
            e.printStackTrace();
        }
    }

    public HashRing(){
        first = null;
        size = 0;
    }

    class MetaDataNode {
        public String name;
        public String host;
        public int port;
        public MetaDataNode(String name, String host, int port){
            this.name = name;
            this.host = host;
            this.port = port;
        }
    }

    public HashRing(String jsonString) {
        System.out.print("------------In Hashing: " + jsonString);
        first = null;
        size = 0;
        Gson gson = new Gson();
//        Type ECSNodeList = new TypeToken<ArrayList<ECSNode>>(){}.getType();
        Type MetaDataList = new TypeToken<List<MetaDataNode>>(){}.getType();
        List<MetaDataNode> temp = gson.fromJson(jsonString, MetaDataList);

//        for (ECSNode ecsnode: temp) {
//            addNode(ecsnode);
//        }
        for (MetaDataNode ecsnode: temp) {
            addNode(new ECSNode(ecsnode.name, ecsnode.host, ecsnode.port));
        }
    }

    public static String getHash(ECSNode node) {
        return HashRing.getHash(node.getNodeHost() + ":" + node.getNodePort());
    }

    public static String getHash(String input){
        md.reset();
        md.update(input.getBytes());
        BigInteger output = new BigInteger(1, md.digest());
        return output.toString(16);
    }

    public ECSNode getNext(ECSNode node) {
        BigInteger next = new BigInteger(getHash(node), 16);
        next = next.add(new BigInteger("1", 16));
        return getNodeByKey(next.toString(16));
    }

    public boolean addNode(ECSNode node) {
        if (checkNodeExist(node)) {
            return false;
        }
        if (first == null) {
            first = node;
            node.setPrevious(node);
        } else {
            ECSNode next = getNodeByKey(getHash(node));
            ECSNode previous = next.getPrevious();
            next.setPrevious(node);
            node.setPrevious(previous);
        }
        size++;
        logger.info("Add ECSNode " + node.getNodeName() + ":" + node.getNodePort());
        return true;
    }

    public boolean checkNodeExist(ECSNode node) {
        String hash = getHash(node);
        ECSNode curr = first;
        for(int i = 0; i < size; i++) {
            if (getHash(curr).equals(hash)) return true;
            curr = curr.getPrevious();
        }
        return false;
    }


    public boolean removeNode(ECSNode node) {
        if(!checkNodeExist(node)) {
            return false;
        }
        ECSNode actual = getNodeByKey(getHash(node));
        if (node.getPrevious() == node) {
            first = null;
        } else {
            BigInteger next = new BigInteger(getHash(node), 16);
            next = next.add(new BigInteger("1", 16));
            ECSNode nextNode = getNodeByKey(next.toString(16));
            ECSNode previousNode = actual.getPrevious();
            nextNode.setPrevious(previousNode);

            if(first == node) first = nextNode;
        }
        size--;
        logger.info("Remove ECSNode " + node.getNodeName() + ":" + node.getNodePort());
        return true;
    }

    public ECSNode getNodeByKey(String hashString) {
//        System.out.print("------------In getNodeByKey 1: " + hashString);
        BigInteger hash = new BigInteger(hashString, 16);
//        System.out.print("------------In getNodeByKey 2");
        ECSNode curr = first;
        for(int i = 0; i < size; i++) {
//            System.out.print("------------In getNodeByKey 3");
            String[] range = curr.getNodeHashRange();
            BigInteger lower = new BigInteger(range[0], 16);
            BigInteger upper = new BigInteger(range[1], 16);

            if (upper.compareTo(lower) <= 0) {
//                System.out.print("------------In getNodeByKey 4");
                if (hash.compareTo(upper) <= 0 || hash.compareTo(lower) > 0) {
//                    System.out.print("------------In getNodeByKey 5");
                    return curr;
                }
            } else if (hash.compareTo(lower) > 0 && hash.compareTo(upper) <= 0) {
//                System.out.print("------------In getNodeByKey 6");
                return curr;
            }
            curr = curr.getPrevious();
        }
        logger.warn("Fail to get the node with hash "+ hashString);
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        sb.append("[");

        ECSNode curr = first;
        do {
            gson.toJson(curr);
            curr = curr.getPrevious();
        } while (curr != first);

        sb.append("]");
        return sb.toString();
    }

    public void removeAll() {
        size = 0;
        first = null;
    }


    public static boolean isInRange(String[] hashRange, String key) {
        String hash = HashRing.getHash(key);
        BigInteger temp = new BigInteger(hash, 16);
        BigInteger lower = new BigInteger(hashRange[0]);
        BigInteger upper = new BigInteger(hashRange[1]);

        if (upper.compareTo(lower) <= 0) {
            return temp.compareTo(upper) <= 0 || temp.compareTo(lower) > 0;
        } else {
            return temp.compareTo(upper) >= 0 || temp.compareTo(lower) < 0;
        }
    }


}
