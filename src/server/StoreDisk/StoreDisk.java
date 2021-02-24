package server.StoreDisk;

import ecs.HashRing;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Scanner;

public class StoreDisk implements IStoreDisk {

    private static Logger LOG = Logger.getRootLogger();
    private static String CLASS_NAME = "StoreDisk: ";

    private File storage;
    private File toMove;
//    private String resourceDir = System.getProperty("user.dir") + "/Desktop/419/ECE419-2021/src/resources/";
//    private String resourceDir = System.getProperty("user.dir") + "/Documents/ECE419/ECE419-2021Winter-Project" + "/ECE419-2021/src/resources/";
    private String resourceDir = "./src/resources/";
    private String filename;
    private String toMoveFileName;



    public StoreDisk(String filename){
        System.out.println("!!!!!!!!!!!!!!!!!!Path: " + resourceDir);
        LOG.info(CLASS_NAME+"Initiate the persistent storage for "+filename);
        System.out.println(CLASS_NAME+"Initiate the persistent storage for "+filename);

        this.filename = filename + ".txt";
        this.toMoveFileName = filename + "_toMove" + ".txt";
        this.storage = new File(resourceDir + this.filename);
        this.toMove = new File(resourceDir + this.toMoveFileName);
        try {
            System.out.println("-------------------File path is: " + this.storage.getPath());
//            System.out.println("-------------------Move path name is: " + this.toMove.getParent());
            if(this.storage.createNewFile()){
                LOG.info(CLASS_NAME+"File successfully created");
            }else{
                LOG.info(CLASS_NAME+"File already existed");
            }
        } catch (IOException e) {
            LOG.error(CLASS_NAME+"Error for creating file",e);
        }

    }
    public void delete(String key, String value)throws Exception{
        File newFile = new File(resourceDir+"temp_"+filename);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(storage));
            BufferedWriter writer = new BufferedWriter(new FileWriter(newFile));

            String lineToRemove = key;
            String line;
            while((line = reader.readLine())!= null){
                line = line.trim();
                String[] lineArray = line.split(":");
                if(lineArray[0].equals(lineToRemove)) continue;
                writer.write(line+System.lineSeparator());
            }
            writer.close();
            reader.close();

            if(!storage.delete()){
                System.out.println("wtf");
            }
            newFile.renameTo(storage);
            this.storage = new File(resourceDir+this.filename);

        } catch (IOException e) {
            LOG.error(CLASS_NAME+"Error for deleting file",e);
            throw new Exception(CLASS_NAME+"Error for deleting file");
        }

    }

    @Override
    public File filter(String[] hashRange) throws Exception{
        System.out.println("--------In filter---------");
        try {
            toMove.delete();
            toMove.createNewFile();
        } catch (IOException e) {
            LOG.error(e);
            throw new Exception(CLASS_NAME + "fail to create to-move data.");
        }
        try {
            Scanner scanner = new Scanner(this.storage);
            FileWriter fw = new FileWriter(resourceDir+this.toMoveFileName, true);
            while(scanner.hasNextLine()){
                String line = scanner.nextLine();
                line = line.trim();
                if(line.isEmpty()){
                    continue;
                }
                String[] lineArray = line.split(":");
                if (lineArray.length != 2){
                    LOG.info(CLASS_NAME+"Invalid found of the line");
                    break;
                }
                if(HashRing.isInRange(hashRange, lineArray[0])){
                    fw.write(lineArray[0]+":"+lineArray[1]+"\n");
                    delete(lineArray[0], lineArray[1]);
                }
            }
            fw.write("\r");
            System.out.println("--------In filter 4---------");
            scanner.close();
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(CLASS_NAME+"Error for move file to to-move file",e);
            throw new Exception(CLASS_NAME+"Error for move file to to-move file");
        }
        return toMove;
    }

    public void dump(){
        storage.delete();
        storage = new File(resourceDir+this.filename);
        try {
            storage.createNewFile();
        } catch (IOException e) {
            LOG.error(CLASS_NAME+"Error for creating file during dumping",e);
        }
    }


    @Override
    public void put(String key, String value)throws Exception {
        String orgValue = get(key);
        if(orgValue != null){
            delete(key,orgValue);
        }
        try {
            FileWriter fw = new FileWriter(resourceDir+this.filename, true);
            fw.write(key+":"+value+"\n");
            fw.close();

        } catch (IOException e) {
            LOG.error(CLASS_NAME+"Error for put a new KV pair",e);
            throw new Exception(CLASS_NAME+"Error for iterating  file");
        }
    }

    @Override
    public String get(String key) throws Exception{
        String value = null;
        try {
            Scanner scanner = new Scanner(this.storage);
            while(scanner.hasNextLine()){
                String line = scanner.nextLine();
                line = line.trim();
                if(line.isEmpty()){
                    LOG.info(CLASS_NAME+"File already existed");
                    continue;
                }
                String[] lineArray = line.split(":");
                if (lineArray.length != 2){
                    LOG.info(CLASS_NAME+"Invalid found of the line");
                    break;
                }
//                System.out.println(lineArray[0]);
                if(lineArray[0].equals(key)){
                    value = lineArray[1];
                    break;
                }
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            LOG.error(CLASS_NAME+"Error for iterating  file",e);
            throw new Exception(CLASS_NAME+"Error for iterating  file");
        }
        return value;
    }

    @Override
    public boolean contain(String key) {
        String value = null;
        try {
            value = get(key);
        } catch (Exception e) {
            LOG.error(CLASS_NAME+"Error for contain function",e);
        }
        if(value == null){
            return false;
        }
        return true;
    }

//    public static void main(String args[]){
//        StoreDisk sd = new StoreDisk("testing.txt");
//        sd.put("K2","v2");
//        sd.put("K2","v");
//        sd.put("K1","v1");
//        sd.put("K3","v3");
//        String t = sd.get("K2");
//        System.out.println(t);
//        System.out.println(sd.get("K3"));
//        sd.dump();
//        sd.put("K2","v2");
//        sd.put("K2","v");
//        sd.put("K1","v1");
//        sd.put("K3","v3");
//        System.out.println(sd.get("K2"));
//
//    }
}
