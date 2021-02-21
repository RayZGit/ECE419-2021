package ecs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import org.apache.log4j.Logger;

public class HashRing {
    private static Logger logger = Logger.getRootLogger();
    private static MessageDigest md;
    private ECSNode first;
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

    public HashRing(String jsonString) {
        first = null;
        size = 0;
        Gson gson = new Gson();
        Type ECSNodeList = new TypeToken<ArrayList<ECSNode>>(){}.getType();
        ECSNode[] temp = gson.fromJson(jsonString, ECSNodeList);

        for (ECSNode ecsnode: temp) {
            addNode(ecsnode);
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

    public void addNode(ECSNode node) {
        if (first == null) {
            first = node;
            node.setPrevious(node);
        } else {
            ECSNode next = getNodeByKey(node.getNodeName() + ":" + node.getNodePort());
            ECSNode previous = next.getPrevious();

            next.setPrevious(node);
            node.setPrevious(previous);
            size++;
        }
    }

    public boolean checkNodeExist(String hash) {
        ECSNode curr = first;
        for(int i = 0; i < size; i++) {
            if (getHash(curr).equals(hash)) return true;
        }
        return false;
    }

    public void removeNode(String hash) {
        if (checkNodeExist(hash)) {
            ECSNode node = getNodeByKey(hash);
            removeNode(node);
        } else {
            logger.warn("ECSNode does not exist!");
        }
    }

    public void removeNode(ECSNode node) {
        if (node.getPrevious() == node) {
            first = null;
        } else {
            BigInteger next = new BigInteger(getHash(node), 16);
            next = next.add(new BigInteger("1", 16));
            ECSNode nextNode = getNodeByKey(next.toString());
            nextNode.setPrevious(node.getPrevious());
            if(first == node) first = nextNode;
            size--;
        }
    }

    public ECSNode getNodeByKey(String key) {
        BigInteger hash = new BigInteger(getHash(key), 16);
        ECSNode curr = first;
        for(int i = 0; i < size; i++) {
            String[] range = curr.getNodeHashRange();
            BigInteger lower = new BigInteger(range[0], 16);
            BigInteger upper = new BigInteger(range[1], 16);

            if (upper.compareTo(lower) <= 0) {
                if (hash.compareTo(upper) <= 0 || hash.compareTo(lower) >= 0) {
                    return curr;
                }
            } else if (hash.compareTo(upper) >= 0 || hash.compareTo(lower) <= 0) {
                return curr;
            }
            curr = curr.getPrevious();
        }
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
