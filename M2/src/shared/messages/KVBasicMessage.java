package shared.messages;

import java.io.Serializable;

public class KVBasicMessage implements KVMessage, Serializable {
    String key;
    String value;
    StatusType status;

    public KVBasicMessage(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public KVBasicMessage() {
        this.key = null;
        this.value = null;
        this.status = null;
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
    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public void setStatus(StatusType status) {
        this.status = status;
    }
}
