package kr.co.dealmungchi.hotdealbatch.service.filehandler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Local filesystem implementation of the FileHandler interface.
 * Stores files on the local filesystem with a directory structure based on key prefixes.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LocalFileHandler implements FileHandler {

    @Value("${file.upload-dir}")
    private String baseDir;

    /**
     * Uploads a file to the local filesystem.
     * The file is stored in a subdirectory based on the first two characters of the key.
     *
     * @param key The key (filename) to store the file under
     * @param data The binary data to store
     * @return The URI of the uploaded file as a string
     * @throws RuntimeException if the file cannot be written
     */
    @Override
    public String upload(String key, byte[] data) {
        try {
            // Create the target file path with subdirectory based on first 2 chars of key
            Path directoryPath = createDirectoryPath(key);
            Path filePath = directoryPath.resolve(key);
            
            // Ensure the directory exists
            Files.createDirectories(directoryPath);
            
            // Write the file
            try (FileOutputStream fos = new FileOutputStream(filePath.toFile())) {
                fos.write(data);
            }
            
            log.debug("File uploaded successfully to {}", filePath);
            return filePath.toUri().toString();
        } catch (IOException e) {
            log.error("[LocalFileHandler] Failed to upload file {}", key, e);
            throw new RuntimeException("Failed to write local file: " + e.getMessage(), e);
        }
    }

    /**
     * Creates the directory path for a file based on its key.
     *
     * @param key The key (filename)
     * @return The directory path
     */
    private Path createDirectoryPath(String key) {
        // Use first 2 characters of key as subdirectory for better file organization
        if (key.length() < 2) {
            return Paths.get(baseDir);
        }
        return Paths.get(baseDir, key.substring(0, 2));
    }

    /**
     * Deletes a file from the local filesystem.
     *
     * @param key The key (filename) to delete
     */
    @Override
    public void delete(String key) {
        try {
            Path directoryPath = createDirectoryPath(key);
            Path filePath = directoryPath.resolve(key);
            
            boolean deleted = Files.deleteIfExists(filePath);
            if (!deleted) {
                log.warn("[LocalFileHandler] File not found for deletion: {}", key);
            } else {
                log.debug("File deleted successfully: {}", key);
            }
        } catch (IOException e) {
            log.warn("[LocalFileHandler] Failed to delete file: {}", key, e);
        }
    }
}

