package kr.co.dealmungchi.hotdealbatch.service;

import kr.co.dealmungchi.hotdealbatch.entity.Provider;
import kr.co.dealmungchi.hotdealbatch.entity.ProviderType;
import kr.co.dealmungchi.hotdealbatch.repository.ProviderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProviderCacheService {

    private final ProviderRepository providerRepository;
    private final Map<ProviderType, Provider> providerCache = new ConcurrentHashMap<>();

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        refreshCache();
    }

    public void refreshCache() {
        log.debug("Refreshing provider cache");
        providerCache.clear();
        providerRepository.findAll().forEach(provider -> 
            providerCache.put(provider.getProviderType(), provider));
        log.debug("Provider cache refreshed with {} entries", providerCache.size());
    }

    public Provider getProvider(ProviderType providerType) {
        Provider provider = providerCache.get(providerType);
        if (provider == null) {
            log.info("Provider for {} not found in cache, fetching from database", providerType);
            provider = providerRepository.findByProviderType(providerType)
                    .orElseThrow(() -> new IllegalArgumentException("Unknown provider: " + providerType));
            providerCache.put(providerType, provider);
        }
        return provider;
    }

    public Provider getProvider(String providerEn) {
        ProviderType providerType = ProviderType.from(providerEn);
        return getProvider(providerType);
    }
}