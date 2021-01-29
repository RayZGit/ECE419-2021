package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			new Thread(new KVServer(50000, 5, "FIFO")).start();
//			new Thread(new KVServer(50000, 5, "LRU")).start();
//			new Thread(new KVServer(50000, 5, "None")).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(AdditionalTest.class);
		clientSuite.addTestSuite(StorageTest.class);
		clientSuite.addTestSuite(FIFOCacheTest.class);
//		clientSuite.addTestSuite(ConcurrencyTest.class);
		return clientSuite;
	}
	
}
