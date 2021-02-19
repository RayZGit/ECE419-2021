package ecs;


import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;

import java.math.BigInteger;

public class ECSNode implements IECSNode{
    @Expose
    private String name;
    @Expose
    private String host;
    @Expose
    private int port;
    private ECSNode previous;

    @Override
    public String getNodeName() {
        return name;
    }

    public ECSNode(String name, String host, int port) {
        this.name = name;
        this.host = host;
        this.port = port;
        previous = null;
    }

    @Override
    public String getNodeHost() {
        return host;
    }

    @Override
    public int getNodePort() {
        return port;
    }

    @Override
    public String[] getNodeHashRange() {
        if (previous == this)
            return new String[]{"00000000000000000000000000000000", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"};
        String start = HashRing.getHash(previous);
        String end = HashRing.getHash(this);
        return new String[]{start, end};
    }


    public ECSNode getPrevious() {
        return previous;
    }

    public void setPrevious(ECSNode previous) {
        this.previous = previous;
    }

    public boolean isInRange(String key) {
        String hash = HashRing.getHash(key);
        BigInteger temp = new BigInteger(hash, 16);
        String[] range = this.getNodeHashRange();
        BigInteger lower = new BigInteger(range[0]);
        BigInteger upper = new BigInteger(range[1]);

        if (upper.compareTo(lower) <= 0) {
            return temp.compareTo(upper) <= 0 || temp.compareTo(lower) > 0;
        } else {
            return temp.compareTo(upper) >= 0 || temp.compareTo(lower) < 0;
        }
    }

}
