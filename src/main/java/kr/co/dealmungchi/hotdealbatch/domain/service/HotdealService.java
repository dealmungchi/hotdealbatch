package kr.co.dealmungchi.hotdealbatch.domain.service;

import org.springframework.stereotype.Service;

import kr.co.dealmungchi.hotdealbatch.service.filehandler.FileHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for managing hot deal-related operations.
 * Currently handles thumbnail uploads.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class HotdealService {

    private final FileHandler fileHandler;
    
    /**
     * Uploads a thumbnail image and returns its path.
     *
     * @param hash The hash identifier for the thumbnail, used as the filename
     * @param data The binary data of the thumbnail image
     * @return The path where the thumbnail was uploaded
     */
    public String uploadThumbnail(String hash, byte[] data) {
        String path = fileHandler.upload(hash, data);
        log.debug("Thumbnail uploaded to path: {}", path);
        return path;
    }
}
