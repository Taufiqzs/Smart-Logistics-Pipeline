from __future__ import annotations


# Pemetaan eksplisit berdasarkan metadata dan nama lokasi stasiun OpenAQ.
#
# Jangan menetapkan stasiun ke kota hanya berdasarkan
# kedekatan geografis dengan kota operasional.

# Pemetaan ID lokasi OpenAQ ke kota operasional yang digunakan dalam proyek.
OPENAQ_CITY_MAPPING = {
    # Jakarta
    2537: "Jakarta",
    2538: "Jakarta",
    8320: "Jakarta",
    8637: "Jakarta",
    223262: "Jakarta",
    223824: "Jakarta",
    1776543: "Jakarta",
    1894639: "Jakarta",
    2071976: "Jakarta",
    3276989: "Jakarta",
    3280117: "Jakarta",
    3294802: "Jakarta",
    6051931: "Jakarta",
    6051932: "Jakarta",
    6051933: "Jakarta",
    6409860: "Jakarta",
    6455006: "Jakarta",

    # Bandung
    1285347: "Bandung",
    3056210: "Bandung",

    # Yogyakarta
    3037147: "Yogyakarta",
    4047633: "Yogyakarta",

    # Medan
    5586536: "Medan",
}


# Mencari kota operasional berdasarkan ID lokasi OpenAQ.
def get_operational_city(location_id: int) -> str | None:
    return OPENAQ_CITY_MAPPING.get(int(location_id))