package kr.co.dealmungchi.hotdealbatch.service.filehandler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

@Slf4j
@Component
@RequiredArgsConstructor
public class LocalFileHandler implements FileHandler {

    @Value("${file.upload-dir}")
    private String baseDir;

    @Override
    public String upload(String key, byte[] data) {
        try {
            String separator = File.separator;
            File target = new File(baseDir + separator + key.substring(0, 2) + separator + key);
            target.getParentFile().mkdirs();
            try (FileOutputStream fos = new FileOutputStream(target)) {
                fos.write(data);
            }
            return target.toURI().toString();
        } catch (IOException e) {
            log.error("[LocalFileHandler] Failed to upload file {}", key, e);
            throw new RuntimeException("Failed to write local file", e);
        }
    }

    @Override
    public void delete(String key) {
        File target = new File(baseDir + File.separator + key);
        if (target.exists() && !target.delete()) {
            log.warn("[LocalFileHandler] Failed to delete file: {}", key);
        }
    }
}

