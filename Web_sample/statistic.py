from pymongo import MongoClient
import datetime

AREA_TYPE_MAPPING = {
    "loại hình khác": ['Nhỏ hơn 120 m2', 'Từ 120 m2 đến 500 m2', 'Từ 500 m2 đến 2.5 ha', 'Từ 2.5 ha đến 4.0 ha',
                       'Từ 4.0 ha đến 6.2 ha', 'Từ 6.2 ha đến 8.5 ha', 'Từ 8.5 ha đến 11.0 ha', 'Lớn hơn 11.0 ha'],
    "đất nền": ['Nhỏ hơn 80 m2', 'Từ 80 m2 đến 210 m2', 'Từ 210 m2 đến 360 m2', 'Từ 360 m2 đến 700 m2',
                'Từ 700 m2 đến 1.0 ha', 'Từ 1.0 ha đến 1.4 ha', 'Từ 1.4 ha đến 1.9 ha', 'Lớn hơn 1.9 ha'],
    "biệt thự": ['Nhỏ hơn 70 m2', 'Từ 70 m2 đến 100 m2', 'Từ 100 m2 đến 130 m2', 'Từ 130 m2 đến 165 m2',
                 'Từ 165 m2 đến 200 m2', 'Từ 200 m2 đến 250 m2', 'Từ 250 m2 đến 300 m2', 'Lớn hơn 300 m2'],
    "nhà trong hẻm": ['Nhỏ hơn 40 m2', 'Từ 40 m2 đến 55 m2', 'Từ 55 m2 đến 70 m2', 'Từ 70 m2 đến 80 m2',
                      'Từ 80 m2 đến 90 m2', 'Từ 90 m2 đến 100 m2', 'Từ 100 m2 đến 150 m2', 'Lớn hơn 150 m2'],
    "nhà mặt tiền": ['Nhỏ hơn 50 m2', 'Từ 50 m2 đến 70 m2', 'Từ 70 m2 đến 90 m2', 'Từ 90 m2 đến 110 m2',
                     'Từ 110 m2 đến 140 m2', 'Từ 140 m2 đến 180 m2', 'Từ 180 m2 đến 230 m2', 'Lớn hơn 230 m2'],
    "căn hộ chung cư": ['Nhỏ hơn 40 m2', 'Từ 40 m2 đến 55 m2', 'Từ 55 m2 đến 70 m2', 'Từ 70 m2 đến 80 m2',
                        'Từ 80 m2 đến 90 m2', 'Từ 90 m2 đến 100 m2', 'Từ 100 m2 đến 120 m2', 'Lớn hơn 120 m2'],
    "khách sạn, cửa hàng": ['Nhỏ hơn 80 m2', 'Từ 80 m2 đến 120 m2', 'Từ 120 m2 đến 170 m2', 'Từ 170 m2 đến 260 m2',
                            'Từ 260 m2 đến 350 m2', 'Từ 350 m2 đến 490 m2', 'Từ 490 m2 đến 620 m2', 'Lớn hơn 620 m2'],
    "phòng trọ, nhà trọ": ['Nhỏ hơn 50 m2', 'Từ 50 m2 đến 65 m2', 'Từ 65 m2 đến 80 m2', 'Từ 80 m2 đến 95 m2',
                           'Từ 95 m2 đến 110 m2', 'Từ 110 m2 đến 130 m2', 'Từ 130 m2 đến 160 m2', 'Lớn hơn 160 m2'],
    "văn phòng": ['Nhỏ hơn 120 m2', 'Từ 120 m2 đến 300 m2', 'Từ 300 m2 đến 550 m2', 'Từ 550 m2 đến 780 m2',
                  'Từ 780 m2 đến 1.0 ha', 'Từ 1.0 ha đến 1.5 ha', 'Từ 1.5 ha đến 1.9 ha', 'Lớn hơn 1.9 ha'],
    "Tất cả loại hình": []
}

PRICE_TYPE_MAPPING = {
    "loại hình khác": ['Nhỏ hơn 4.0 tỷ', 'Từ 4.0 tỷ đến 8.0 tỷ', 'Từ 8.0 tỷ đến 12.0 tỷ', 'Từ 12.0 tỷ đến 20.0 tỷ',
                       'Từ 20.0 tỷ đến 40.0 tỷ', 'Từ 40.0 tỷ đến 60.0 tỷ', 'Từ 60.0 tỷ đến 80.0 tỷ', 'Lớn hơn 80.0 tỷ'],
    "đất nền": ['Nhỏ hơn 1.5 tỷ', 'Từ 1.5 tỷ đến 6.0 tỷ', 'Từ 6.0 tỷ đến 10.0 tỷ', 'Từ 10.0 tỷ đến 15.0 tỷ',
                'Từ 15.0 tỷ đến 20.0 tỷ', 'Từ 20.0 tỷ đến 30.0 tỷ', 'Từ 30.0 tỷ đến 80.0 tỷ', 'Lớn hơn 80.0 tỷ'],
    "biệt thự": ['Nhỏ hơn 5.0 tỷ', 'Từ 5.0 tỷ đến 8.0 tỷ', 'Từ 8.0 tỷ đến 14.0 tỷ', 'Từ 14.0 tỷ đến 20.0 tỷ',
                 'Từ 20.0 tỷ đến 35.0 tỷ', 'Từ 35.0 tỷ đến 50.0 tỷ', 'Từ 50.0 tỷ đến 60.0 tỷ', 'Lớn hơn 60.0 tỷ'],
    "nhà trong hẻm": ['Nhỏ hơn 3.0 tỷ', 'Từ 3.0 tỷ đến 4.0 tỷ', 'Từ 4.0 tỷ đến 5.0 tỷ', 'Từ 5.0 tỷ đến 7.0 tỷ',
                      'Từ 7.0 tỷ đến 9.0 tỷ', 'Từ 9.0 tỷ đến 12.0 tỷ', 'Từ 12.0 tỷ đến 15.0 tỷ', 'Lớn hơn 15.0 tỷ'],
    "nhà mặt tiền": ['Nhỏ hơn 3.0 tỷ', 'Từ 3.0 tỷ đến 7.0 tỷ', 'Từ 7.0 tỷ đến 15.0 tỷ', 'Từ 15.0 tỷ đến 20.0 tỷ',
                     'Từ 20.0 tỷ đến 35.0 tỷ', 'Từ 35.0 tỷ đến 45.0 tỷ', 'Từ 45.0 tỷ đến 80.0 tỷ', 'Lớn hơn 80.0 tỷ'],
    "căn hộ chung cư": ['Nhỏ hơn 600 triệu', 'Từ 600 triệu đến 1.2 tỷ', 'Từ 1.2 tỷ đến 2.0 tỷ', 'Từ 2.0 tỷ đến 3.5 tỷ',
                        'Từ 3.5 tỷ đến 5.0 tỷ', 'Từ 5.0 tỷ đến 7.5 tỷ', 'Từ 7.5 tỷ đến 10.0 tỷ', 'Lớn hơn 10.0 tỷ'],
    "khách sạn, cửa hàng": ['Nhỏ hơn 19.0 tỷ', 'Từ 19.0 tỷ đến 40.0 tỷ', 'Từ 40.0 tỷ đến 70.0 tỷ', 'Từ 70.0 tỷ đến 95.0 tỷ',
                            'Từ 95.0 tỷ đến 200.0 tỷ', 'Từ 200.0 tỷ đến 400.0 tỷ', 'Từ 400.0 tỷ đến 600.0 tỷ', 'Lớn hơn 600.0 tỷ'],
    "phòng trọ, nhà trọ": ['Nhỏ hơn 2.0 tỷ', 'Từ 2.0 tỷ đến 5.0 tỷ', 'Từ 5.0 tỷ đến 7.5 tỷ', 'Từ 7.5 tỷ đến 10.0 tỷ',
                           'Từ 10.0 tỷ đến 13.0 tỷ', 'Từ 13.0 tỷ đến 17.0 tỷ', 'Từ 17.0 tỷ đến 25.0 tỷ', 'Lớn hơn 25.0 tỷ'],
    "văn phòng": ['Nhỏ hơn 10.0 tỷ', 'Từ 10.0 tỷ đến 20.0 tỷ', 'Từ 20.0 tỷ đến 30.0 tỷ', 'Từ 30.0 tỷ đến 65.0 tỷ',
                  'Từ 65.0 tỷ đến 100.0 tỷ', 'Từ 100.0 tỷ đến 180.0 tỷ', 'Từ 180.0 tỷ đến 260.0 tỷ', 'Lớn hơn 260.0 tỷ'],
    "Tất cả loại hình": []
}


def get_statistic_data(STATISTICAL_CLIENT: MongoClient, prop_type, district, ward,
                       is_post, day, area_type, price_type):
    min_time = int(datetime.datetime.strptime("16/12/21 00:00:00", "%d/%m/%y %H:%M:%S").timestamp()) - day*86400
    filter = {"property_linux": {"$gt": min_time}}
    if prop_type != "Tất cả loại hình":
        filter["property_type"] = prop_type
    if district != "Quận/huyện":
        filter["property_district"] = district
    if ward != "Phường/xã":
        filter["property_ward"] = ward
    if area_type != "Phân khúc diện tích":
        filter["property_area_type"] = area_type
    if price_type != "Phân khúc giá cả":
        filter["property_price_type"] = price_type
    try:
        statistic_db = STATISTICAL_CLIENT.get_database("StatisticalData")
        if is_post:
            statistic_collect = statistic_db.get_collection("PostSumup")
        else:
            statistic_collect = statistic_db.get_collection("ReadSumup")

        data = statistic_collect.find(filter)
        data = list(data)
        if len(data) > 0:
            for x in data:
                x["_id"] = str(x["_id"])
            return True, "Lấy dữ liệu thành công", data
        else:
            return False, "Không có dữ liệu thị trường", []
    except Exception as ex:
        return False, "Lấy dữ liệu thất bại", []
