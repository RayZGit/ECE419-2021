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

    public KVBasicMessage(String key, String value, String status) {
        this(key, value, StatusType.valueOf(status));
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
}
