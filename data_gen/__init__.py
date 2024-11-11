from faker.providers import DynamicProvider

geo_area_provider = DynamicProvider(
    provider_name="geo_area",
    elements=[
        "North Africa",
        "West Africa",
        "East Africa",
        "Central Africa",
        "Southern Africa"
        "Central Asia",
        "East Asia",
        "South Asia",
        "Southeast Asia",
        "West Asia"
        "Western Europe",
        "Eastern Europe",
        "Northern Europe",
        "Southern Europe"
        "Northern America",
        "Central America",
        "Caribbean"
        "South America (general)",
        "Northern South America",
        "Southern Cone"
        "Australasia",
        "Melanesia",
        "Micronesia",
        "Polynesia"
    ]
)

store_type_provider = DynamicProvider(
    provider_name="store_type",
    elements=[
        "Supermarket",
        "Bakery",
        "Butcher Shop",
        "Cafe",
        "Restaurant",
        "Wine & Liquor Store",
        "Convenience Store",
        "Department Store",
        "Boutique",
        "Shoe Store",
        "Jewelry Store",
        "Vintage Clothing Store",
        "Thrift Store",
        "Electronics Store",
        "Game Store",
        "Music Store",
        "Movie Rental Store",
        "Mobile Phone Store",
        "Pharmacy",
        "Health Food Store",
        "Gym/Fitness Center",
        "Yoga Studio",
        "Optometrist/Optical Store",
        "Home Decor Store",
        "Furniture Store",
        "Hardware Store",
        "Garden Center",
        "DIY Store",
        "Bookstore",
        "Pet Store",
        "Travel Agency",
        "Bank Branch",
        "Post Office"
    ]
)
