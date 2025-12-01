import requests

API_KEY = "d494b1a1af4978a9adbbe5eb9e39698b"


def test_coord(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    res = requests.get(url)
    data = res.json()
    print("City:", city)
    print("Coord:", data.get("coord"))
    print("Temp:", data["main"]["temp"])
    print("Humidity:", data["main"]["humidity"])
    print("Weather:", data["weather"][0]["description"])
    print("-" * 40)


if __name__ == "__main__":
    for city in ["Hanoi", "Ho Chi Minh City", "Da Nang", "Haiphong", "Can Tho"]:
        test_coord(city)
