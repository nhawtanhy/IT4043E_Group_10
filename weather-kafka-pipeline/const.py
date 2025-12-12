CITY_LIST = ["Hanoi", "Ho Chi Minh City", "Da Nang", "Haiphong", "Can Tho"]
TOPIC = "weather_raw"
WMO_CODE_MAP = {
    0: "clear sky",
    1: "mainly clear",
    2: "partly cloudy",
    3: "overcast",
    45: "fog",
    48: "depositing rime fog",
    51: "light drizzle",
    53: "moderate drizzle",
    55: "dense drizzle",
    56: "light freezing drizzle",
    57: "dense freezing drizzle",
    61: "slight rain",
    63: "moderate rain",
    65: "heavy rain",
    66: "light freezing rain",
    67: "heavy freezing rain",
    71: "slight snow fall",
    73: "moderate snow fall",
    75: "heavy snow fall",
    77: "snow grains",
    80: "slight rain showers",
    81: "moderate rain showers",
    82: "violent rain showers",
    85: "slight snow showers",
    86: "heavy snow showers",
    95: "thunderstorm",
    96: "thunderstorm with slight hail",
    99: "thunderstorm with heavy hail"
}
CITY_COORDS = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Ho Chi Minh City": {"lat": 10.8231, "lon": 106.6297},
    "Da Nang": {"lat": 16.0544, "lon": 108.2022},
    "Haiphong": {"lat": 20.8449, "lon": 106.6881},
    "Can Tho": {"lat": 10.0452, "lon": 105.7469},
}
KAFKA = "kafka:9092"
ES_INDEX = "weather-data"