package server.StoreDisk;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Scanner;

public class StoreDisk implements IStoreDisk {

    private static Logger LOG = Logger.getRootLogger();
    private static String CLASS_NAME = "StoreDisk: ";

    private File storage;
    private String resourceDir = "./src/resources/";;
    private String filename;



    public StoreDisk(String filename){
//        LOG.info(CLASS_NAME+"Initiate the persistent storage for "+filename);
        System.out.println(CLASS_NAME+"Initiate the persistent storage for "+filename);

        this.filename = filename;
         this.storage = new File(resourceDir+this.filename);
        try {
            if(this.storage.createNewFile()){
//                LOG.info(CLASS_NAME+"File successfully created");
            }else{
//                LOG.info(CLASS_NAME+"File already existed");
            }
        } catch (IOException e) {
//            LOG.error(CLASS_NAME+"Error for creating file",e);
        }

    }
    private void delete(String key, String value){
        File newFile = new File(resourceDir+"temp_"+filename);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(storage));
            BufferedWriter writer = new BufferedWriter(new FileWriter(newFile));

            String lineToRemove = key+":"+value;
            String line;
            while((line = reader.readLine())!= null){
                line = line.trim();
                if(line.equals(lineToRemove)) continue;
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
//            LOG.error(CLASS_NAME+"Error for deleting file",e);
        }

    }
    private void dump(){
        storage.delete();
        storage = new File(resourceDir+this.filename);
        try {
            storage.createNewFile();
        } catch (IOException e) {
//            LOG.error(CLASS_NAME+"Error for creating file during dumping",e);
        }
    }


    @Override
    public void put(String key, String value) {
        String orgValue = get(key);
        if(orgValue != null){
            delete(key,orgValue);
        }
        try {
            FileWriter fw = new FileWriter(resourceDir+this.filename, true);
            fw.write(key+":"+value+"\n");
            fw.close();

        } catch (IOException e) {
//            LOG.error(CLASS_NAME+"Error for put a new KV pair",e);
        }



    }

    @Override
    public String get(String key) {
        String value = null;
        try {
            Scanner scanner = new Scanner(this.storage);
            while(scanner.hasNextLine()){
                String line = scanner.nextLine();
                line = line.trim();
                if(line.isEmpty()){
//                    LOG.info(CLASS_NAME+"File already existed");
                    continue;
                }
                String[] lineArray = line.split(":");
                if (lineArray.length != 2){
//                    LOG.info(CLASS_NAME+"Invalid found of the line");
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
//            LOG.error(CLASS_NAME+"Error for iterating  file",e);
        }
        return value;
    }

    @Override
    public boolean contain(String key) {
        String value = get(key);
        if(value == null){
            return false;
        }
        return true;
    }

    public static void main(String args[]){
        StoreDisk sd = new StoreDisk("testing.txt");
        sd.put("K2","v2");
        sd.put("K2","v");
        sd.put("K1","v1");
        sd.put("K3","v3");
        String t = sd.get("K2");
        System.out.println(t);
        System.out.println(sd.get("K3"));
        sd.dump();
        System.out.println(sd.get("K3"));

    }
}
