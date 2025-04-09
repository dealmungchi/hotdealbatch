package kr.co.dealmungchi.hotdealbatch.service.filehandler

import io.awspring.cloud.s3.S3Operations
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.ByteArrayInputStream

/**
 * Amazon S3 implementation of the FileHandler interface.
 * Stores files in S3 buckets with organizational prefixes based on key values.
 */
@Component
class S3FileHandler(
    private val s3Operations: S3Operations,

    @Value("\${spring.cloud.aws.s3.bucket}")
    private val bucket: String
) : FileHandler {
    
    private val log = LoggerFactory.getLogger(S3FileHandler::class.java)
    
    /**
     * Path separator for S3 object keys
     */
    private val s3PathSeparator = "/"

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
    override fun upload(key: String, data: ByteArray): String {
        return try {
            // Determine target location with organizational prefix
            val prefix = getPrefix(key)
            val s3Key = "$prefix$s3PathSeparator$key"
            
            // Upload the file to S3
            ByteArrayInputStream(data).use { inputStream ->
                s3Operations.upload(bucket, s3Key, inputStream)
                log.debug("File uploaded successfully to S3: s3://{}/{}", bucket, s3Key)
                "s3://$bucket/$s3Key"
            }
        } catch (e: Exception) {
            log.error("[S3FileHandler] Failed to upload file {}", key, e)
            throw RuntimeException("Failed to upload file to S3: ${e.message}", e)
        }
    }

    /**
     * Deletes a file from Amazon S3.
     *
     * @param key The key (filename) to delete
     */
    override fun delete(key: String) {
        try {
            // Determine the full S3 key including prefix
            val prefix = getPrefix(key)
            val s3Key = "$prefix$s3PathSeparator$key"
            
            s3Operations.deleteObject(bucket, s3Key)
        } catch (e: Exception) {
            log.warn("[S3FileHandler] Failed to delete file {} from S3", key, e)
        }
    }
    
    /**
     * Gets the organizational prefix for a key.
     * Uses the first two characters of the key for even distribution.
     *
     * @param key The key (filename)
     * @return The organizational prefix
     */
    private fun getPrefix(key: String): String =
        if (key.length < 2) "" else key.substring(0, 2)
}