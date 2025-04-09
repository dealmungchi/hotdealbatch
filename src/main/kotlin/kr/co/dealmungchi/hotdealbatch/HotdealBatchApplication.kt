package kr.co.dealmungchi.hotdealbatch

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.data.jpa.repository.config.EnableJpaAuditing

/**
 * Main application class for the HotDeal batch processing service.
 */
@SpringBootApplication
@EnableJpaAuditing
class HotdealBatchApplication

/**
 * Application entry point.
 */
fun main(args: Array<String>) {
    runApplication<HotdealBatchApplication>(*args)
}