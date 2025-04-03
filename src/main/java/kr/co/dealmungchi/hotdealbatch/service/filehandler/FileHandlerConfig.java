package kr.co.dealmungchi.hotdealbatch.service.filehandler;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FileHandlerConfig {

    @Bean
    public FileHandler fileHandler() {
        return new LocalFileHandler();
    }
}
