package testing;

import shared.messages.KVMessage;
import ecs.HashRing;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Before;

public class HashRingTest extends TestCase{
    private static HashRing hashRing = new HashRing();
    private Exception e;


    public void testAddNode() {
        ECSNode node1 = new ECSNode("first", "localhost", 10000);
        ECSNode node2 = new ECSNode("third", "localhost", 10001);
        ECSNode node3 = new ECSNode("fourth", "localhost", 10002);
        hashRing.addNode(node1);
        hashRing.addNode(node2);
        hashRing.addNode(node3);
        assert (hashRing.getSize() == 3);
        assert (node1.getPrevious() == node2 || node1.getPrevious() == node3);
        assert (hashRing.getNodeByKey(HashRing.getHash(node1)) == node1);

        boolean result = hashRing.addNode(new ECSNode("second", "localhost", 10000));
        assert (!result);
    }

    public void testRemoveNode() {
        ECSNode node1 = new ECSNode("first", "localhost", 10000);
        ECSNode node2 = new ECSNode("third", "localhost", 10001);
        ECSNode node3 = new ECSNode("fourth", "localhost", 10002);
        hashRing.addNode(node1);
        hashRing.addNode(node2);
        hashRing.addNode(node3);

        hashRing.removeNode(new ECSNode("fourth", "localhost", 10001));

        assert(hashRing.getSize() == 2);
        assert(node1.getPrevious() == node3);
        assert(node3.getPrevious() == node1);

    }
}
