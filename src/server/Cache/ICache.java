package server.Cache;

public interface ICache {
    public void put(String key, String value);
    public String get(String key);
    public boolean contain(String key);
    public void dump(String key);

}
