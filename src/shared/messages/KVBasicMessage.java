package shared.messages;

import java.io.Serializable;

public class KVBasicMessage implements KVMessage, Serializable {
    private String key;
    private String value;
    private StatusType status;
    private String text;

    public KVBasicMessage(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }


    public KVBasicMessage(String key, String value, String status) {
        this(key, value, StatusType.valueOf(status));
    }

    public KVBasicMessage(String text) {
        this.text = text;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }

    @Override
    public String getMessages() { return text;}
}
