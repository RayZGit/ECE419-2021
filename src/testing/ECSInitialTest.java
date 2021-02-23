package testing;


import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
//import org.junit.FixMethodOrder;
//import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


public class ECSInitialTest extends TestCase {
    private static Logger logger = Logger.getRootLogger();
    private static final Integer BIG_SERVER_NUM = 1024 * 1024;
    private static final String CACHE_STRATEGY = "FIFO";
    private static final Integer CACHE_SIZE = 1024;

//    public void setUp() throws Exception {
//        super.setUp();
//
//        new LogSetup("./logs/test_ecs.log", Level.ALL);
//
//    }

    private static ECSClient ecs = null;
    private Exception ex = null;

    @Before
    public void preTest() {
        ex = null;
    }

    /*
    Following are the basic functionality test for ECS
    No client data interaction is included
     */
    public void test01Creation() throws IOException, InterruptedException {
        String current = new java.io.File( "." ).getCanonicalPath();
        System.out.println("Current dir:"+current);
        ecs = new ECSClient("./src/resources/ecs.config");

    }

    /**
     * This testcase is no longer in use because ssh-ing to localhost and create
     * server instances is high un-debug-able
     */

    public void test02AddNodes() throws Exception {
        Collection<IECSNode> nodes =
                ecs.setupNodes(BIG_SERVER_NUM, CACHE_STRATEGY, CACHE_SIZE);
        assertNull(nodes);

        Integer count = 3;
        nodes = ecs.setupNodes(count, CACHE_STRATEGY, CACHE_SIZE);
        assertNotNull(nodes);
        assertEquals(count, new Integer(nodes.size()));

        // Start the servers internally
        for (IECSNode node : nodes) {
            new Thread(
                    new KVServer(node.getNodePort(), node.getNodeName(),
                            ECSClient.ZK_HOST, Integer.parseInt(ECSClient.ZK_PORT)))
                    .start();
        }

        boolean ret = ecs.awaitNodes(count, ECSClient.ZK_TIMEOUTSESSION);
        assertTrue(ret);
    }

    /**
     * Start the nodes just added
     */
    public void test03StartNodes() throws Exception {
        assertNotNull(ecs);
        boolean ret = ecs.start();
        assertTrue(ret);

        Collection<IECSNode> nodes = ecs.getNodes().values();
        for (IECSNode node : nodes) {
            assertEquals(ECSNode.NodeStatus.START,
                    ((ECSNode) node).getStatus());
        }
    }

    /**
     * Remove one active node from whole service
     */
    public void test04RemoveNodes() {
        List<String> names = new ArrayList<>(ecs.getNodes().keySet());
        assertTrue(names.size() > 0);
        logger.info("Removing " + names.get(0) + " node");
        boolean ret = ecs.removeNodes(Collections.singletonList(names.get(0)));
        assertTrue(ret);
    }

    /**
     * Stop all active nodes
     */
    public void test05Stop() throws Exception {
        boolean ret = ecs.stop();
        assertTrue(ret);
    }

    /**
     * Shut down all nodes
     */
    public void test06Shutdown() throws Exception {
        boolean ret = ecs.shutdown();
        assertTrue(ret);
    }

}