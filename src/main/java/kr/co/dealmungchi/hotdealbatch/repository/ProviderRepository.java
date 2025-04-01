package kr.co.dealmungchi.hotdealbatch.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import kr.co.dealmungchi.hotdealbatch.entity.Provider;
import kr.co.dealmungchi.hotdealbatch.entity.ProviderType;

public interface ProviderRepository extends JpaRepository<Provider, Long> {

    Optional<Provider> findByProviderType(ProviderType providerType);
}
