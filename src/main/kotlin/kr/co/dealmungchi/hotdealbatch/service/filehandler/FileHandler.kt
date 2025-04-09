package kr.co.dealmungchi.hotdealbatch.service.filehandler

/**
 * Interface for file storage operations.
 * Provides methods for uploading and deleting files.
 */
interface FileHandler {
    /**
     * Uploads a file with the given key and data.
     *
     * @param key The key (filename) to store the file under
     * @param data The binary data to store
     * @return The path or URI where the file was uploaded
     * @throws RuntimeException if the upload fails
     */
    fun upload(key: String, data: ByteArray): String

    /**
     * Deletes a file with the given key.
     *
     * @param key The key (filename) to delete
     */
    fun delete(key: String)
}