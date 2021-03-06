package testing;

import app_kvServer.KVServer;
import client.KVStore;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Performance {
    KVServer kvServer;
    int total;
    int type;
    int port;
    File file;

    public Performance (int port, int cacheSize, String strategy, int total) {
        try {
            file = new File("performance.txt");
            if (file.createNewFile()) {
                System.out.println("File created: " + file.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        this.port = port;
        kvServer = new KVServer(port, cacheSize, strategy);
        this.total = total;
        new Thread(kvServer).start();
    }

    public void setType(int type) {
        this.type = type;
    }

    public void testLatency(){
        KVStore client = new KVStore("localhost", port);
        Random rand = new Random();
        try {
            client.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        int putCount = 0;
        int getCount = 0;
        long putTime = 0;
        long getTime = 0;
        int flag = rand.nextInt(10);
        int putkey = rand.nextInt(100);
        int putVal = rand.nextInt(10000);
        int getKey = rand.nextInt(100);


        long highest = -1;
        try {
            client.put(Integer.toString(putkey), Integer.toString(putVal));
            client.put(Integer.toString(putkey + 1), Integer.toString(putVal));
        } catch (Exception e) {
            e.printStackTrace();
        }

        long start = System.nanoTime();

        for (int i = 0; i < total; i++) {
            try {
                boolean put;
                flag = rand.nextInt(10);
                putVal = rand.nextInt(10000);
                getKey = rand.nextInt(100);
                long temp1 = System.nanoTime();

                if (type == 1) {
                    if (flag < 5) {
                        client.put(Integer.toString(generateRandomKey()), Integer.toString(putVal));
                        putCount += 1;
                        put = true;
                    }
                    else {
                        client.get(Integer.toString(getKey));
                        getCount += 1;
                        put = false;
                    }
                } else if (type == 2) {
                    if (flag < 2) {
                        client.put(Integer.toString(generateRandomKey()), Integer.toString(putVal));
                        putCount += 1;
                        put = true;
                    }
                    else {
                        client.get(Integer.toString(getKey));
                        getCount += 1;
                        put = false;
                    }
                } else {
                    if (flag < 8) {
                        client.put(Integer.toString(generateRandomKey()), Integer.toString(putVal));
                        putCount += 1;
                        put = true;
                    }
                    else {
                        client.get(Integer.toString(getKey));
                        getCount += 1;
                        put = false;
                    }
                }

                long temp2 = System.nanoTime();
                long time1 = temp2 - temp1;

                if (time1 > highest) highest = time1;

                if (put) putTime += time1;
                else getTime += time1;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        long end = System.nanoTime();
        long timePass = end - start;
        double latency = (double) (timePass/1000000) / total;
        double putLatency = (double) (putTime/1000000) / putCount;
        double getLatency = (double) (getTime/1000000) / getCount;
        double throughput =  total / (double) (timePass/1000000);
        try {
            FileWriter fw = new FileWriter("performance.txt", true);
            fw.write("Highest latency of the operations is: " + highest / 1000000 + "ms.\n");
            fw.write("Average latency is: " + latency + " ms. \n");
            fw.write("Average latency for put is: " + Math.round(putLatency * 10000.0) / 10000.0 + " ms. \n");
            fw.write("Average latency for get is: " + Math.round(getLatency * 10000.0) / 10000.0 + " ms. \n");
            fw.write("Average throughput is: " + Math.round(throughput * 1000 * 100.0) / 100.0 + " operations per second. \n");
            fw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int generateRandomKey(){
        Random rand = new Random();
        double result = rand.nextGaussian() * 25 + 50;
        return (int) result;
    }

    public void testMultiClientThroughput(int numClient) {
        Thread[] threads = new Thread[numClient];
        for (int i = 0; i < numClient; i++) {
            threads[i] =  new Thread(new Runnable() {
                KVStore client;

                @Override
                public void run() {
                    client = new KVStore("localhost", port);
                    Random rand = new Random();
                    try {
                        client.connect();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    for(int j = 0; j < total; j++){
                        try {
                            int flag = rand.nextInt(10);
                            int putkey = rand.nextInt(100);
                            int putVal = rand.nextInt(10000);
                            int getKey = rand.nextInt(100);
                            if (type == 1) {
                                if (flag < 5) client.put(Integer.toString(putkey), Integer.toString(putVal));
                                else client.get(Integer.toString(getKey));
                            } else if (type == 2) {
                                if (flag < 2) client.put(Integer.toString(putkey), Integer.toString(putVal));
                                else client.get(Integer.toString(getKey));
                            } else {
                                if (flag < 8) client.put(Integer.toString(putkey), Integer.toString(putVal));
                                else client.get(Integer.toString(getKey));
                            }
                        }
                        catch (Exception e){
                            e.printStackTrace();
                        }

                    }
                }
            });
        }
        long start = System.nanoTime();
        for (int i = 0; i < numClient; i++) {
            threads[i].start();
        }
        for (int i = 0; i < numClient; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long end = System.nanoTime();
        long timePass = end - start;
        double throughput = total * numClient / (double) (timePass/1000000);
        try {
            FileWriter fw = new FileWriter("performance.txt", true);
            fw.write("Multi-client throughput is: " + throughput + " operations per ms.");
            fw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println();
    }

    public static void main(String[] args) {
        int cacheSize = 200;
        int port = 50010;

        String cacheStrategy = "LRU";
        Performance performance = new Performance(port, cacheSize, cacheStrategy, 10000);
        performance.setType(1);
        try {
            FileWriter fw = new FileWriter("performance.txt", true);
            fw.write("\n-----------------New Test-----------------\n");
            fw.write("Testing with cacheSize: " + cacheSize + "\n");
            fw.write("Testing with caStrategy: " + cacheStrategy + "\n");
            fw.write("\nTesting 50% put, 50% get: \n");
            fw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        //performance.testMultiClientThroughput(10);
        performance.testLatency();
        performance.setType(2);
        try {
            FileWriter fw = new FileWriter("performance.txt", true);
            fw.write("\nTesting 20% put, 80% get: \n");
            fw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println();
        //performance.testMultiClientThroughput(10);
        performance.testLatency();
        performance.setType(3);
        try {
            FileWriter fw = new FileWriter("performance.txt", true);
            fw.write("\nTesting 80% put, 20% get: \n");
            fw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        //performance.testMultiClientThroughput(10);
        performance.testLatency();
    }
}
