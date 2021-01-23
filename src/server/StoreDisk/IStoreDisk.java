package server.StoreDisk;

public interface IStoreDisk {
    public void put(String key, String value);
    public void get(String key);
    public boolean contain(String key);
}
