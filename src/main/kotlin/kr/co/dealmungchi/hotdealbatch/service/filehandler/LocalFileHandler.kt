package kr.co.dealmungchi.hotdealbatch.service.filehandler

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Local filesystem implementation of the FileHandler interface.
 * Stores files on the local filesystem with a directory structure based on key prefixes.
 */
@Component
class LocalFileHandler(
    @Value("\${file.upload-dir}")
    private val baseDir: String
) : FileHandler {
    
    private val log = LoggerFactory.getLogger(LocalFileHandler::class.java)

    /**
     * Uploads a file to the local filesystem.
     * The file is stored in a subdirectory based on the first two characters of the key.
     *
     * @param key The key (filename) to store the file under
     * @param data The binary data to store
     * @return The URI of the uploaded file as a string
     * @throws RuntimeException if the file cannot be written
     */
    override fun upload(key: String, data: ByteArray): String {
        return try {
            // Create the target file path with subdirectory based on first 2 chars of key
            val directoryPath = createDirectoryPath(key)
            val filePath = directoryPath.resolve(key)
            
            // Ensure the directory exists
            Files.createDirectories(directoryPath)
            
            // Write the file
            FileOutputStream(filePath.toFile()).use { fos ->
                fos.write(data)
            }
            
            filePath.toUri().toString()
        } catch (e: Exception) {
            log.error("[LocalFileHandler] Failed to upload file {}", key, e)
            throw RuntimeException("Failed to write local file: ${e.message}", e)
        }
    }

    /**
     * Creates the directory path for a file based on its key.
     *
     * @param key The key (filename)
     * @return The directory path
     */
    private fun createDirectoryPath(key: String): Path {
        // Use first 2 characters of key as subdirectory for better file organization
        return if (key.length < 2) {
            Paths.get(baseDir)
        } else {
            Paths.get(baseDir, key.substring(0, 2))
        }
    }

    /**
     * Deletes a file from the local filesystem.
     *
     * @param key The key (filename) to delete
     */
    override fun delete(key: String) {
        try {
            val directoryPath = createDirectoryPath(key)
            val filePath = directoryPath.resolve(key)
            
            val deleted = Files.deleteIfExists(filePath)
            if (!deleted) {
                log.warn("[LocalFileHandler] File not found for deletion: {}", key)
            } else {
                log.debug("File deleted successfully: {}", key)
            }
        } catch (e: Exception) {
            log.warn("[LocalFileHandler] Failed to delete file: {}", key, e)
        }
    }
}