package kr.co.dealmungchi.hotdealbatch.domain.entity;

import lombok.Getter;
import java.util.Arrays;

@Getter
public enum ProviderType {
    ARCA("Arca"),
    CLIEN("Clien"),
    COOLANDJOY("Coolandjoy"),
    DAMOANG("Damoang"),
    FMKOREA("FMKorea"),
    PPOMPPUEN("PpomEn"),
    PPOMPPU("Ppom"),
    QUASAR("Quasar"),
    RULIWEB("Ruliweb"),
    ;

    private String providerEn;

    ProviderType(String providerEn) {
        this.providerEn = providerEn;
    }

    public static ProviderType from(String providerEn) {
        return Arrays.stream(values())
            .filter(value -> value.providerEn.equals(providerEn))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown providerEn: " + providerEn));
    }
}
