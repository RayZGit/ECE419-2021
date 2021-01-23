package server.StoreDisk;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;

public class StoreDisk implements IStoreDisk {

    private static Logger LOG = Logger.getRootLogger();
    private static String CLASS_NAME = "StoreDisk: ";

    private RandomAccessFile storage;
    private String resourceDir;



    public StoreDisk(String filename){
        LOG.info(CLASS_NAME+"Initiate the persistent storage for "+filename);



    }


    @Override
    public void put(String key, String value) {

    }

    @Override
    public void get(String key) {

    }

    @Override
    public boolean contain(String key) {

        return false;
    }
}
