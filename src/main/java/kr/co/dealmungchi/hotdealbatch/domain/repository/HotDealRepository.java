package kr.co.dealmungchi.hotdealbatch.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import kr.co.dealmungchi.hotdealbatch.domain.entity.HotDeal;

import java.util.List;

public interface HotDealRepository extends JpaRepository<HotDeal, Long> {
    boolean existsByLink(String link);
    List<HotDeal> findAllByLinkIn(List<String> links);
}

