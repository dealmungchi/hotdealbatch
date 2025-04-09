package kr.co.dealmungchi.hotdealbatch.service.filehandler

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Configuration for file handlers.
 * Defines which FileHandler implementation to use.
 */
@Configuration
class FileHandlerConfig(
    private val localFileHandler: LocalFileHandler
) {

    /**
     * Provides the FileHandler implementation to use for the application.
     * Currently configured to use LocalFileHandler.
     * To switch to S3FileHandler, modify this method to return an instance of S3FileHandler.
     *
     * @return The FileHandler implementation to use
     */
    @Bean
    fun fileHandler(): FileHandler = localFileHandler
}