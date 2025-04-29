package kr.co.dealmungchi.hotdealbatch.domain.entity

/**
 * Enum class representing product categories for hot deals.
 * Each category has a main category name.
 */
enum class CategoryType(val categoryName: String) {
    ELECTRONICS_DIGITAL_PC("전자제품/디지털/PC/하드웨어"),
    SOFTWARE_GAME("소프트웨어/게임"),
    HOUSEHOLD_INTERIOR_KITCHEN("생활용품/인테리어/주방"),
    FOOD("식품/먹거리"),
    CLOTHING_FASHION_ACCESSORIES("의류/패션/잡화"),
    COSMETICS_BEAUTY("화장품/뷰티"),
    BOOKS_MEDIA_CONTENTS("도서/미디어/콘텐츠"),
    CAMERA_PHOTO("카메라/사진"),
    VOUCHER_COUPON_POINT("상품권/쿠폰/포인트"),
    BABY_CHILDCARE("출산/육아"),
    PET("반려동물"),
    SPORTS_OUTDOOR_LEISURE("스포츠/아웃도어/레저"),
    HEALTH_VITAMIN("건강/비타민"),
    TRAVEL_SERVICE("여행/서비스"),
    EVENT_ENTRY_VIRAL("이벤트/응모/바이럴"),
    SCHOOL_OFFICE_SUPPLIES("학용품/사무용품"),
    ETC("기타");

    companion object {
        /**
         * Find a product category type by its name.
         *
         * @param name The name to search for
         * @return The corresponding product category type or ETC if not found
         */
        fun fromName(name: String): CategoryType {
            return values().find { it.categoryName == name } ?: ETC
        }
    }
} 