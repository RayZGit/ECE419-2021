package testing;

import client.KVStore;
import org.junit.Test;

import junit.framework.TestCase;
import org.w3c.dom.Text;
import shared.KVMsgProtocol;
import shared.messages.KVBasicMessage;
import shared.messages.KVMessage;
import shared.messages.TextMessage;

import java.util.Random;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	
	@Test
	public void testStub() {
		assertTrue(true);
	}

	public void testKeyLength() {
		KVStore kvClient = new KVStore("localhost", 50000);
		String key = "ThisStringHasSizeLargerThanTwentyBytes";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	public void testValueLength() {
		KVStore kvClient = new KVStore("localhost", 50000);
		String key = "foo";
		byte[] array = new byte[150 * 1024];
		new Random().nextBytes(array);
		String value = new String(array);
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	public void testEncode() {
		String key = "foo";
		String value = "bar";
		KVMessage kvMessage = new KVBasicMessage(key, value, KVMessage.StatusType.GET);
		TextMessage textMessage = KVMsgProtocol.encode(kvMessage);

		assertEquals("GET foo bar", textMessage.getMsg());
	}

	public void testDecode() {
		String msg = "GET foo bar";
		TextMessage textMessage = new TextMessage(msg);
		KVMessage kvMessage = KVMsgProtocol.decode(textMessage);

		assertEquals("foo", kvMessage.getKey());
		assertEquals("bar", kvMessage.getValue());
		assertEquals(KVMessage.StatusType.GET, kvMessage.getStatus());
	}
}
