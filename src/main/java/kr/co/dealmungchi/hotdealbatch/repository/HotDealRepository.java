package kr.co.dealmungchi.hotdealbatch.repository;

import kr.co.dealmungchi.hotdealbatch.entity.HotDeal;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface HotDealRepository extends JpaRepository<HotDeal, Long> {
    boolean existsByLink(String link);
    List<HotDeal> findAllByLinkIn(List<String> links);
}

