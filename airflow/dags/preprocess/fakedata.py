import pandas as pd
import numpy as np
from faker import Faker

fake = Faker()

# Sample data based on the provided example
data = [
    {
        "Acreage": 82.0,
        "Contact_address": "Đống Đa, Hà Nội",
        "Contact_email": "dotien20900@gmail.com",
        "Contact_name": "Đỗ Xuân Tiến",
        "Contact_phone": "0901766566",
        "Crawled_date": "18-04-2024",
        "Created_date": "2024-04-18",
        "Description": "Bán Gấp! Tòa Nhà Vip Quận Ba Đình, Phố Phan Kế Bính, 82m x 10T, Mt 7m. Giá 34.5 tỷ. KHÁCH SẠN 10 TẦNG 22 PHÒNG KINH DOANH - Ô TÔ TRÁNH VỈA HÈ - THÔNG CÁC NGẢ- Khu vực tập trung khách Hàn Quốc, Nhật Bản cao cấp, dòng tiền đều đặn. Thông các ngõ Linh Lang, Nguyễn Văn Ngọc.- Khách sạn thiết kế 10 tầng thang máy, cầu thang giữa ( tổng 24 phòng ):T1: Lễ TânT2: 2 Phòng rộngT3,4,5,6,7,8: Mỗi tầng 3 phòng kinh doanhT9,10: Mỗi tầng 2 phòng rộng- Sổ đỏ vuông vắn, pháp lý rõ ràng. Chủ đang gửi ngân hàng.",
        "Direction": None,
        "Horizontal_length": 12.0,
        "Legal": "Giấy đỏ",
        "Location": "Phan Kế Bính, Ba Đình, Hà Nội",
        "Num_floors": 10.0,
        "Num_rooms": 24.0,
        "Num_toilets": 24.0,
        "Post_id": 24041201012,
        "Price": 34500000000.0,
        "RE_Type": "Bán nhà riêng",
        "Title": "Bán Gấp! Tòa Nhà Vip Quận Ba Đình, Phố Phan Kế Bính, 82m x 10T, Mt 7m. Giá 34.5 tỷ.",
        "URL": "https://alomuabannhadat.vn/ban-gap-toa-nha-vip-quan-ba-dinh-pho-phan-ke-binh-82m-x-10t-mt-7m-gia-345-ty-1201012.html",
        "Vertical_length": 7.0,
        "Standardized_Location": "[{'match_address': 'Phúc Xá, Ba Đình, Hà Nội', 'match_percent': 100}, {'match_address': 'Trúc Bạch, Ba Đình, Hà Nội', 'match_percent': 100}, {'match_address': 'Vĩnh Phúc, Ba Đình, Hà Nội', 'match_percent': 100}, {'match_address': 'Cống Vị, Ba Đình, Hà Nội', 'match_percent': 100}, {'match_address': 'Liễu Giai, Ba Đình, Hà Nội', 'match_percent': 100}, {'match_address': 'Nguyễn Trung Trực, Ba Đình, Hà Nội', 'match_percent': 100}, {'match_address': 'Quán Thánh, Ba Đình, Hà Nội', 'match_percent': 100}, {'match_address': 'Ngọc Hà, Ba Đình, Hà Nội', 'match_percent': 100}, {'match_address': 'Điện Biên, Ba Đình, Hà Nội', 'match_percent': 100}, {'match_address': 'Đội Cấn, Ba Đình, Hà Nội', 'match_percent': 100}, {'match_address': 'Ngọc Khánh, Ba Đình, Hà Nội', 'match_percent': 100}, {'match_address': 'Kim Mã, Ba Đình, Hà Nội', 'match_percent': 100}, {'match_address': 'Giảng Võ, Ba Đình, Hà Nội', 'match_percent': 100}, {'match_address': 'Thành Công, Ba Đình, Hà Nội', 'match_percent': 100}, {'match_address': 'Phú Cường, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Cổ Đô, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Tản Hồng, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Vạn Thắng, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Châu Sơn, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Phong Vân, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Phú Phương, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Phú Châu, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Thái Hòa, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Đồng Thái, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Phú Sơn, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Minh Châu, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Vật Lại, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Tòng Bạt, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Sơn Đà, Ba Vì, Hà Nội', 'match_percent': 86}, {'match_address': 'Đông Quang, Ba Vì, Hà Nội', 'match_percent': 86}]",
        "Best_Match_Location": "Phúc Xá, Ba Đình, Hà Nội",
        "Ward": "Phúc Xá",
        "District": "Ba Đình",
        "City": "Hà Nội",
        "Street_size": None,
        "Contact_city": None,
        "Contact_type": None,
        "Type": None,
        "Source": None
    }
]

# Generate 1000 rows of data
rows = []
for i in range(1000):
    row = data[0].copy()
    row["Acreage"] = np.random.choice([60.0, 82.0, 106.0])
    row["Contact_address"] = fake.address()
    row["Contact_email"] = fake.email()
    row["Contact_name"] = fake.name()
    row["Contact_phone"] = fake.phone_number()
    row["Description"] = fake.text(max_nb_chars=200)
    row["Location"] = fake.address()
    row["Price"] = np.random.uniform(1000000000, 50000000000)
    row["Post_id"] = fake.random_number(digits=10)
    row["Title"] = fake.sentence(nb_words=10)
    row["URL"] = fake.url()
    row["Ward"] = fake.word()
    row["District"] = fake.city()
    row["City"] = fake.state()
    rows.append(row)

# Create DataFrame
df = pd.DataFrame(rows)

# Save to CSV
df.to_csv("./processed_data/generated_data.csv", index=False)