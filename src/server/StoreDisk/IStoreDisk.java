package server.StoreDisk;

public interface IStoreDisk {

    public void put(String key, String value) throws Exception;
    public String get(String key) throws Exception;
    public boolean contain(String key);
    public void dump();
    public void delete(String key, String value) throws Exception;
    public String filter(String[] hashRange) throws Exception;
}
