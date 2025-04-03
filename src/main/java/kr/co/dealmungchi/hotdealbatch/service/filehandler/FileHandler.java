package kr.co.dealmungchi.hotdealbatch.service.filehandler;

public interface FileHandler {
    String upload(String key, byte[] data);

    void delete(String key);
}
