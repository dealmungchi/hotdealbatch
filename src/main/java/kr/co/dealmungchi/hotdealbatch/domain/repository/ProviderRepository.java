package kr.co.dealmungchi.hotdealbatch.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import kr.co.dealmungchi.hotdealbatch.domain.entity.Provider;
import kr.co.dealmungchi.hotdealbatch.domain.entity.ProviderType;

public interface ProviderRepository extends JpaRepository<Provider, Long> {

    Optional<Provider> findByProviderType(ProviderType providerType);
}
