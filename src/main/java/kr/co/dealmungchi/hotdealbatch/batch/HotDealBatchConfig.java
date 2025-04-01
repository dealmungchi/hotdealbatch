package kr.co.dealmungchi.hotdealbatch.batch;

import kr.co.dealmungchi.hotdealbatch.dto.HotDealDto;
import kr.co.dealmungchi.hotdealbatch.entity.HotDeal;
import kr.co.dealmungchi.hotdealbatch.repository.HotDealRepository;
import kr.co.dealmungchi.hotdealbatch.service.ProviderCacheService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.JpaTransactionManager;

import java.util.List;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class HotDealBatchConfig {

    private final HotDealRepository repository;
    private final ProviderCacheService providerCacheService;
    
    @Bean
    public Job hotDealJob(JobRepository jobRepository, org.springframework.batch.core.Step step) {
        return new JobBuilder("hotDealJob", jobRepository)
                .start(step)
                .build();
    }

    @Bean
    public org.springframework.batch.core.Step step(JobRepository jobRepository) {

        return new StepBuilder("hotDealStep", jobRepository)
            .<HotDealDto, HotDeal>chunk(10, new JpaTransactionManager())
            .reader(reader())
            .processor(processor())
            .writer(writer())
            .build();
    }

    @Bean
    public ItemReader<HotDealDto> reader() {
        return new ListItemReader<>(List.of());
    }

    @Bean
    public ItemProcessor<HotDealDto, HotDeal> processor() {
        return dto -> {
            if (repository.existsByLink(dto.link())) {
                return null;
            }
            return HotDeal.fromDto(dto, providerCacheService.getProvider(dto.provider()));
        };
    }

    @Bean
    public ItemWriter<HotDeal> writer() {
        return items -> repository.saveAll(items);
    }
}
