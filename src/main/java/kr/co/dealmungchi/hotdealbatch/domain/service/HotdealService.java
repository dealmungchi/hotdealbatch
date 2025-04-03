package kr.co.dealmungchi.hotdealbatch.domain.service;

import org.springframework.stereotype.Service;

import kr.co.dealmungchi.hotdealbatch.service.filehandler.FileHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class HotdealService {

    private final FileHandler fileHandler;
    
    public String uploadThumbnail(String hash, byte[] data) {
        String path = fileHandler.upload(hash, data);

        log.debug("Thumbnail uploaded to path: {}", path);
        return path;
    }
}
