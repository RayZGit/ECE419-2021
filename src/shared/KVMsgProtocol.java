package shared;

import org.apache.log4j.Logger;
import shared.messages.TextMessage;
import shared.messages.KVBasicMessage;
import shared.messages.KVMessage;


import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public abstract class KVMsgProtocol {
    public InputStream inputStream;
    public OutputStream outputStream;
    public static final int BUFFER_SIZE = 50 + 120 * 1024;
    public final Logger logger = Logger.getRootLogger();

    public void sendMessage(KVMessage message) throws Exception{
        outputStream.write(KVMsgProtocol.encode(message).getMsgBytes());
        outputStream.flush();
        logger.info("Send message:\t '" + KVMsgProtocol.encode(message).getMsg() + "'");
    }

    public KVMessage receiveMessage() throws Exception{
        int index = 0;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) inputStream.read();

        int i = 0;
        while(read != 13 && i < BUFFER_SIZE) {/* carriage return */
            bufferBytes[index] = read;
            index++;
            /* read next char from stream */
            read = (byte) inputStream.read();
            i++;
        }
        /* build final String */
        TextMessage textMessage = new TextMessage(bufferBytes);
        String temp = textMessage.getMsg().trim();
        logger.info("Receive message:\t '" + textMessage.getMsg().trim() + "'");
        return KVMsgProtocol.decode(textMessage);
    }

    public static TextMessage encode(KVMessage message) {
        StringBuilder msg = new StringBuilder();
        msg.append(message.getStatus().toString());
        msg.append(" ");
        msg.append(message.getKey());
        msg.append(" ");
        msg.append(message.getValue());
        return new TextMessage(msg.toString());
    }

    public static KVMessage decode(TextMessage textMessage) {
        String[] tokens = textMessage.getMsg().trim().split("\\s+");

        if (tokens.length <= 2) {
            return new KVBasicMessage(tokens[1], null, KVMessage.StatusType.valueOf(tokens[0]));
        }

        String value = String.join(" ", Arrays.copyOfRange(tokens, 2, tokens.length));
        return new KVBasicMessage(tokens[1], value, KVMessage.StatusType.valueOf(tokens[0]));
    }
}
