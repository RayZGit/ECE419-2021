package testing;

import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.KVMessage;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;

public class ConcurrencyTest extends TestCase {

    KVStore client1;

    @Test
    public void testPerf() {

        System.out.println("Start Perf Test:");
        client1 = new KVStore("localhost", 50000);
        try {
            client1.connect();

        } catch (Exception e) {
        }

        Thread child1 = new Thread(new Runnable() {
            KVStore client;
            KVMessage response = null;
            Exception ex = null;

            @Override
            public void run() {
                client = new KVStore("localhost", 50000);
                try {
                    client.connect();
                } catch (Exception e) {
                }
                for (int i = 200; i < 300; i++) {
                    try {
                        response = client.put(Integer.toString(i), "Thread1-" + i);
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }
                    assertTrue(response.getStatus() == KVMessage.StatusType.PUT_UPDATE);
                }
                for (int i = 200; i < 300; i++) {
                    try {
                        response = client.get(Integer.toString(i));
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }
                    assertTrue(response.getStatus() == KVMessage.StatusType.GET_SUCCESS);
                }
            }
        });


        Thread child2 = new Thread(new Runnable() {
            KVStore client;
            KVMessage response = null;
            Exception ex = null;


            @Override
            public void run() {
                client = new KVStore("localhost", 50000);
                try {
                    client.connect();
                } catch (Exception e) {
                }
                for (int i = 200; i < 300; i++) {
                    try {
                        response = client.put(Integer.toString(i), "Thread2-" + i+1);
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }
                    assertTrue(response.getStatus() == KVMessage.StatusType.PUT_UPDATE);
                }
                for (int i = 200; i < 300; i++) {
                    try {
                        response = client.get(Integer.toString(i));
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }
                    assertTrue(response.getStatus() == KVMessage.StatusType.GET_SUCCESS);
                }
            }
        });

        Thread child3 = new Thread(new Runnable() {
            KVStore client;
            KVMessage response = null;
            Exception ex = null;


            @Override
            public void run() {
                client = new KVStore("localhost", 50000);
                try {
                    client.connect();
                } catch (Exception e) {
                }
                for (int i = 200; i < 300; i++) {
                    try {
                        response = client.put(Integer.toString(i), "Thread3-" + i+1);
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }
                    assertTrue(response.getStatus() == KVMessage.StatusType.PUT_UPDATE);
                }
                for (int i = 200; i < 300; i++) {
                    try {
                        response = client.get(Integer.toString(i));
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }
                    assertTrue(response.getStatus() == KVMessage.StatusType.GET_SUCCESS);
                }
            }
        });

        Thread child4 = new Thread(new Runnable() {
            KVStore client;
            KVMessage response = null;
            Exception ex = null;


            @Override
            public void run() {
                client = new KVStore("localhost", 50000);
                try {
                    client.connect();
                } catch (Exception e) {
                }
                for (int i = 200; i < 300; i++) {
                    try {
                        response = client.put(Integer.toString(i), "Thread4-" + i+1);
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }
                    assertTrue(response.getStatus() == KVMessage.StatusType.PUT_UPDATE);
                }
                for (int i = 200; i < 300; i++) {
                    try {
                        response = client.get(Integer.toString(i));
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }
                    assertTrue(response.getStatus() == KVMessage.StatusType.GET_SUCCESS);
                }
            }
        });

        Thread child5 = new Thread(new Runnable() {
            KVStore client;
            KVMessage response = null;
            Exception ex = null;


            @Override
            public void run() {
                client = new KVStore("localhost", 50000);
                try {
                    client.connect();
                } catch (Exception e) {
                }
                for (int i = 200; i < 300; i++) {
                    try {
                        response = client.put(Integer.toString(i), "Thread5-" + i+1);
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }
                    assertTrue(response.getStatus() == KVMessage.StatusType.PUT_UPDATE);
                }
                for (int i = 200; i < 300; i++) {
                    try {
                        response = client.get(Integer.toString(i));
                    } catch (Exception exception) {
                        exception.printStackTrace();
                    }
                    assertTrue(response.getStatus() == KVMessage.StatusType.GET_SUCCESS);
                }
            }
        });

        child1.start();
//        child2.start();
//        child3.start();
//        child4.start();
//        child5.start();

        try {
            child1.join();
        }catch (Exception e){
            System.out.println("execption in join1:"+e);
        }

        try {
            child2.join();
        }catch (Exception e){
            System.out.println("execption in join2:"+e);
        }
//
//        try {
//            child3.join();
//        }catch (Exception e){
//            System.out.println("execption in join2:"+e);
//        }
//
//        try {
//            child4.join();
//        }catch (Exception e){
//            System.out.println("execption in join2:"+e);
//        }
//
//        try {
//            child5.join();
//        }catch (Exception e){
//            System.out.println("execption in join2:"+e);
//        }
    }

}
