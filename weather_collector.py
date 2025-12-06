import requests
import os

BUCKET_NAME = 'hust-bucket-storage'
API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Danh sách 34 tỉnh thành (theo đơn vị hành chính - sau sáp nhập)
CITY_LIST = [
    "An Giang",
    "Bac Ninh",
    "Ca Mau",
    "Cao Bang",
    "Can Tho", 
    "Da Nang", 
    "Buon Ma Thuot", #approximate Đắk lắk
    "Dien Bien Phu", #approximate Điện Biên
    "Can Gio", #approximate Đồng Nai
    "Cao Lanh", #approximate Đồng Tháp
    "Gia Lai",
    "Hanoi", 
    "Hà Tĩnh",
    "Haiphong", 
    "Ho Chi Minh City", 
    "Hung Yen",
    "Khánh Hòa",
    "Lai Chau",
    "Da Lat", #approximate Lâm Đồng
    "Lang Son",
    "Lao Cai",
    "Vinh", #approximate Nghệ An
    "Ninh Binh",
    "Phu Tho",
    "Quang Ngai",
    "Cam Pha Mines", #approximate Quảng Ninh
    "Quảng Trị",
    "Son La",
    "Tay Ninh",
    "Thai Nguyen",
    "Thanh Hoa",
    "Hue",
    "Tuyen Quang",
    "Vinh Long"
]

URL_LIST = [
    f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    for city in CITY_LIST
]

def main():
    failed_cities = []
    for i in range(len(CITY_LIST)):
        print(f"Collecting weather data for {CITY_LIST[i]}...")
        response = requests.get(URL_LIST[i])
        if response.status_code == 200:
            data = response.json()

            print(f"Showing data for {CITY_LIST[i]}")
            print(f"Temperature: {data['main']['temp']}°C")
            print(f"Humidity: {data['main']['humidity']}%")
            print(f"Weather: {data['weather'][0]['description']}")
        else:
            failed_cities.append(CITY_LIST[i])

    print("Failed cities:")
    print(failed_cities)
    
if __name__ == "__main__":
    main()
