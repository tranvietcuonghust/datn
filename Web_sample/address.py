from pymongo import MongoClient
from google.cloud import bigquery

def find_district(client: MongoClient, district_id):
    try:
        district_id = int(district_id)
        address_db = client.get_database("VietNamAddress")
        district_collection = address_db.get_collection("Districts")
        data = district_collection.find_one(filter={"code": district_id})
        if data:
            return data["name"]
        else:
            return ""
    except:
        return ""

def find_ward(client: MongoClient, ward_id):
    try:
        ward_id = int(ward_id)
        address_db = client.get_database("VietNamAddress")
        district_collection = address_db.get_collection("Wards")
        data = district_collection.find_one(filter={"code": ward_id})
        if data:
            return data["name"]
        else:
            return ""
    except:
        return ""

def get_list_cities(client: bigquery.Client):
    try:
        query = f"""
            SELECT City
            FROM `bds-warehouse-424208.bds.city`
            Where City is not null
        """
        print(query)
        query_job = client.query(query)  # Make an API request.

        data = [dict(row) for row in query_job.result()]
        # province = data[0]
       
        if data:
            data = list(data)
            result = []
            for x in data:
                result.append([x["City"],x["City"]])
            return True, "Tìm kiếm thành công", result
        else:
            return False, "Không tìm thấy dữ liệu tinh thanh", []

    except Exception as ex:
        print("Lỗi lấy danh sách quận huyện ", ex)
        return False, "Lỗi database server, không thể lấy danh sách quận huyện", []


def get_list_districts(client: bigquery.Client, province_id):
    try:
        # query = f"""
        #     SELECT City
        #     FROM `bds-warehouse-424208.bds.city`
        #     WHERE City = {province_id}
        # """
        # query_job = client.query(query)  # Make an API request.

        # data = [dict(row) for row in query_job.result()]
        # province = data[0]
        # if province:
        City_codes = province_id
        query = f"""
            SELECT District
            FROM `bds-warehouse-424208.bds.district`
            WHERE City = "{City_codes}" and District is not null
        """
        print(query)
        query_job = client.query(query)  # Make an API request.

        data = [dict(row) for row in query_job.result()]
        if data:
            data = list(data)
            result = []
            for x in data:
                result.append([x["District"],x["District"]])
            return True, "Tìm kiếm thành công", result
        else:
            return False, "Không tìm thấy dữ liệu quận huyện", []
    except Exception as ex:
        print("Lỗi lấy danh sách quận huyện ", ex)
        return False, "Lỗi database server, không thể lấy danh sách quận huyện", []

def get_list_wards(client: bigquery.Client, district_id):
    try:
        # query = f"""
        #     SELECT City
        #     FROM `bds-warehouse-424208.bds.city`
        #     WHERE City = {province_id}
        # """
        # query_job = client.query(query)  # Make an API request.

        # data = [dict(row) for row in query_job.result()]
        # province = data[0]
        # if province:
        City_codes = district_id
        query = f"""
            SELECT Ward
            FROM `bds-warehouse-424208.bds.ward`
            WHERE District = "{City_codes}" and Ward is not null
        """
        print(query)
        query_job = client.query(query)  # Make an API request.

        data = [dict(row) for row in query_job.result()]
        if data:
            data = list(data)
            result = []
            for x in data:
                result.append([x["Ward"],x["Ward"]])
            return True, "Tìm kiếm thành công", result
        else:
            return False, "Không tìm thấy dữ liệu quận huyện", []
    except Exception as ex:
        print("Lỗi lấy danh sách quận huyện ", ex)
        return False, "Lỗi database server, không thể lấy danh sách quận huyện", []

