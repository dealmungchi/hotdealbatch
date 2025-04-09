package kr.co.dealmungchi.hotdealbatch.domain.service

import kr.co.dealmungchi.hotdealbatch.service.filehandler.FileHandler
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * Service for managing hot deal-related operations.
 * Currently handles thumbnail uploads.
 */
@Service
class HotdealService(private val fileHandler: FileHandler) {
    
    private val log = LoggerFactory.getLogger(HotdealService::class.java)
    
    /**
     * Uploads a thumbnail image and returns its path.
     *
     * @param hash The hash identifier for the thumbnail, used as the filename
     * @param data The binary data of the thumbnail image
     * @return The path where the thumbnail was uploaded
     */
    fun uploadThumbnail(hash: String, data: ByteArray): String {
        return fileHandler.upload(hash, data)
    }
    
    /**
     * Uploads a thumbnail image and returns its path as a Mono.
     * Reactive version of uploadThumbnail.
     *
     * @param hash The hash identifier for the thumbnail, used as the filename
     * @param data The binary data of the thumbnail image
     * @return A Mono that resolves to the path where the thumbnail was uploaded
     */
    fun uploadThumbnailReactive(hash: String, data: ByteArray): Mono<String> {
        return Mono.fromCallable { uploadThumbnail(hash, data) }
    }
}