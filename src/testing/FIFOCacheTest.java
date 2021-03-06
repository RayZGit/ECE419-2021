package testing;

import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.KVMessage;

public class FIFOCacheTest extends TestCase {

    private KVStore kvClient;

    public void setUp() {
        kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
        }
    }

    public void tearDown() {
        kvClient.disconnect();
    }

    @Test
    public void testPut() {
        String key = "1";
        String value = "FIFO-1";

        KVMessage response = null;
        Exception ex = null;

        try {
            for (int i = 0; i < 100; i++) {
                response = kvClient.put(Integer.toString(i), "FIFO-" + i);
                assertTrue(response.getStatus() == KVMessage.StatusType.PUT_SUCCESS);
            }

        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex==null);
    }

    @Test
    public void testGetSuccess() {
        KVMessage response = null;
        Exception ex = null;

        try {
            for (int i = 0; i < 100; i++) {
                response = kvClient.get(Integer.toString(i));
                assertTrue(response.getStatus() == KVMessage.StatusType.GET_SUCCESS);
            }
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex==null);
    }

    @Test
    public void testGetFailed() {
        KVMessage response = null;
        Exception ex = null;

        try {
            for (int i = 100; i < 200; i++) {
                response = kvClient.get(Integer.toString(i));
                assertTrue(response.getStatus() == KVMessage.StatusType.GET_ERROR);
            }
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex==null);
    }

    @Test
    public void testUpdate() {
        KVMessage response = null;
        Exception ex = null;

        try {
            for (int i = 0; i < 100; i++) {
                response = kvClient.put(Integer.toString(i), "UPDATE FIFO-" + i);
                assertTrue(response.getStatus() == KVMessage.StatusType.PUT_UPDATE);
            }
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex==null);
    }

//    @Test
//    public void testDelete() {
//        KVMessage response = null;
//        Exception ex = null;
//
//        try {
//            for (int i = 0; i < 100; i++) {
//                kvClient.put(Integer.toString(i), "UPDATE FIFO-" + i);
//                response = kvClient.delete(Integer.toString(i));
//                assertTrue(response.getStatus() == KVMessage.StatusType.DELETE_SUCCESS);
//            }
//        } catch (Exception e) {
//            ex = e;
//        }
//        assertTrue(ex==null);
//    }
}
