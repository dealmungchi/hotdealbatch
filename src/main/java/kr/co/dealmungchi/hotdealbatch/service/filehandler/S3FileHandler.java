package kr.co.dealmungchi.hotdealbatch.service.filehandler;

import io.awspring.cloud.s3.S3Operations;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.stereotype.Component;

/**
 * Amazon S3 implementation of the FileHandler interface.
 * Stores files in S3 buckets with organizational prefixes based on key values.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class S3FileHandler implements FileHandler {

    private final S3Operations s3Operations;

    @Value("${spring.cloud.aws.s3.bucket}")
    private String bucket;

    /**
     * Path separator for S3 object keys
     */
    private static final String S3_PATH_SEPARATOR = "/";

    /**
     * Uploads a file to Amazon S3.
     * The file is stored with a prefix based on the first two characters of the key
     * for better organization and performance.
     *
     * @param key The key (filename) to store the file under
     * @param data The binary data to store
     * @return The S3 path where the file was uploaded
     * @throws RuntimeException if the upload fails
     */
    @Override
    public String upload(String key, byte[] data) {
        try {
            // Determine target location with organizational prefix
            String prefix = getPrefix(key);
            String s3Key = prefix + S3_PATH_SEPARATOR + key;
            
            // Upload the file to S3
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data)) {
                s3Operations.upload(bucket, s3Key, inputStream);
                log.debug("File uploaded successfully to S3: s3://{}/{}", bucket, s3Key);
                return String.format("s3://%s/%s", bucket, s3Key);
            }
        } catch (IOException e) {
            log.error("[S3FileHandler] Failed to upload file {} due to I/O error", key, e);
            throw new RuntimeException("Failed to upload file to S3: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("[S3FileHandler] Failed to upload file {} to S3", key, e);
            throw new RuntimeException("Failed to upload file to S3: " + e.getMessage(), e);
        }
    }

    /**
     * Deletes a file from Amazon S3.
     *
     * @param key The key (filename) to delete
     */
    @Override
    public void delete(String key) {
        try {
            // Determine the full S3 key including prefix
            String prefix = getPrefix(key);
            String s3Key = prefix + S3_PATH_SEPARATOR + key;
            
            s3Operations.deleteObject(bucket, s3Key);
            log.debug("File deleted successfully from S3: s3://{}/{}", bucket, s3Key);
        } catch (Exception e) {
            log.warn("[S3FileHandler] Failed to delete file {} from S3", key, e);
        }
    }
    
    /**
     * Gets the organizational prefix for a key.
     * Uses the first two characters of the key for even distribution.
     *
     * @param key The key (filename)
     * @return The organizational prefix
     */
    private String getPrefix(String key) {
        if (key.length() < 2) {
            return "";
        }
        return key.substring(0, 2);
    }
}