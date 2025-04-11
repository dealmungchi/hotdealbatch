package kr.co.dealmungchi.hotdealbatch.domain.entity

/**
 * Sealed class representing the types of providers.
 * Each provider type has a corresponding English name.
 */
enum class ProviderType(val providerEn: String) {
    ARCA("Arca"),
    CLIEN("Clien"),
    COOLANDJOY("Coolandjoy"),
    DAMOANG("Damoang"),
    FMKOREA("FMKorea"),
    PPOMPPUEN("PpomEn"),
    PPOMPPU("Ppom"),
    QUASAR("Quasar"),
    RULIWEB("Ruliweb"),
    DEALBADA("Dealbada")
    ;

    companion object {
        /**
         * Find a provider type by its English name.
         *
         * @param providerEn The English name of the provider
         * @return The corresponding provider type
         * @throws IllegalArgumentException if the provider name is unknown
         */
        fun from(providerEn: String): ProviderType = 
            values().find { it.providerEn == providerEn }
                ?: throw IllegalArgumentException("Unknown providerEn: $providerEn")
    }
}