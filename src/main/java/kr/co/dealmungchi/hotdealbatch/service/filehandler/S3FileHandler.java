package kr.co.dealmungchi.hotdealbatch.service.filehandler;

import io.awspring.cloud.s3.S3Operations;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class S3FileHandler implements FileHandler {

    private final S3Operations s3Operations;

    @Value("${spring.cloud.aws.s3.bucket}")
    private String bucket;

    @Override
    public String upload(String key, byte[] data) {
        try {
            String seperator = File.separator;
            String targetBucket = bucket + seperator + key.substring(0, 2);
            
            ByteArrayResource resource = new ByteArrayResource(data);
            s3Operations.upload(targetBucket, key, resource.getInputStream());
            return targetBucket + seperator + key;
        } catch (Exception e) {
            log.error("[S3FileHandler] Failed to upload file {}", key, e);
            throw new RuntimeException("Failed to upload file to S3", e);
        }
    }

    @Override
    public void delete(String key) {
        try {
            s3Operations.deleteObject(bucket, key);
        } catch (Exception e) {
            log.warn("[S3FileHandler] Failed to delete file {}", key, e);
        }
    }
}