from pymongo import MongoClient
import re
import random
import uuid
import os
from pydrive.drive import GoogleDrive
from werkzeug.datastructures import FileStorage
from log import get_current_time_short_str, get_current_timestamp
from google.cloud import bigquery
import base64
def url_to_id(url):
    return base64.urlsafe_b64encode(url.encode()).decode()
def id_to_url(id):
    # Decode the Base64 back into bytes and then to a string
    return base64.urlsafe_b64decode(id.encode()).decode()
# def find_properties(client : MongoClient, filter = {}, sort = {"property_linux": -1}, offset = 0, limit=15):
#     try:
#         print("filter: ",filter)
#         sort_list = []
#         search = False

#         if "property_search" in filter:
#             query_str = filter["property_search"]
#             filter["$text"] = {"$search": query_str}
#             filter.pop('property_search', None)
#             sort_list.append(('score', {'$meta': 'textScore'}))
#             search = True

#         db = client.get_database("PropertiesDatabase")
#         collection = db.get_collection("MediatedCleanData")

#         for key in sort:
#             sort_list.append((key, sort[key]))

#         if search:
#             data = collection.find(filter, {'score': {'$meta': 'textScore'},}).sort(sort_list).skip(skip=offset).limit(limit=limit)
#         else:
#             data = collection.find(filter = filter).sort(sort_list).skip(skip=offset).limit(limit=limit)

#         data = list(data)
#         if len(data)>0:
#             for x in data:
#                 x["_id"] = str(x["_id"])
#             return True, "Lấy dữ liệu thành công", data
#         else:
#             return False, "Không có dữ liệu tin bài", []
#     except Exception as ex:
#         print("Exception in find items from database", ex)
#         return False, "Lỗi database server. Lấy dữ liệu thất bại", None

def find_properties(client: bigquery.Client, filter={}, sort={"property_linux": -1}, offset=0, limit=15):
    try:
        print("filter: ", filter)

        # Construct WHERE clause from filter
        where_clauses = []
        for k, v in filter.items():
            if k == "property_search":
                continue
            if v:  # Only add non-empty filter values
                where_clauses.append(f"{k} = '{v}'")
        where_clause = "and ".join(where_clauses) if where_clauses else "1=1"  # Default to true if no filters

        # Construct SQL query
        query = f"""
            SELECT *
            FROM `bds-warehouse-424208.bds.bds`
            WHERE {where_clause}
            LIMIT {limit}
        """
        print(query)
        # Run the query
        query_job = client.query(query)
        data = [dict(row) for row in query_job.result()]

        # for item in data:
        #     item['URL'] = url_to_id(item['URL'])
        if data:
            # data["_id"] = str(data["_id"])
            return True, "Tìm kiếm thành công", data
        else:
            return False, "Khong co du lieu", []

    except Exception as e:
        print(f"An error occurred: {e}")
        return False, str(e), []

def get_random_properties(client: MongoClient, limit=6):
    try:
        db = client.get_database("PropertiesDatabase")
        collection = db.get_collection("MediatedCleanData")
        data = collection.aggregate([{"$sample": {'size': limit}}, ])
        data = list(data)
        if len(data)>0:
            for x in data:
                x["_id"] = str(x["_id"])
            return True, "Lấy dữ liệu thành công", data
        else:
            return False, "Không có dữ liệu tin bài", []
    except Exception as ex:
        print("Exception in get random items from database", ex)
        return False, "Lỗi database server. Lấy dữ liệu thất bại", None

def count_properties(client : bigquery.Client, filter = {}):
    try:
        if "property_search" in filter:
            query_str = filter["property_search"]
            filter["$text"] = {"$search": query_str}
            filter.pop('property_search', None)
        db = client.get_database("PropertiesDatabase")
        collection = db.get_collection("MediatedCleanData")
        count = collection.count_documents(filter=filter)
        return True, "Đếm thành công", count
    except:
        return False, "Lỗi database server. Đếm thất bại", 0

def get_property(client: bigquery.Client, id):
    try:
        query = f"""
            SELECT *
            FROM `bds-warehouse-424208.bds.bds`
            WHERE URL = '{id}'
            LIMIT 1
        """

        query_job = client.query(query)  # Make an API request.

        data = [dict(row) for row in query_job.result()]
        data = data[0]
        if data:
            # data["_id"] = str(data["_id"])
            return True, "Tìm kiếm thành công", data
        else:
            return False, "ID bị sai", None
    except:
        return False, "Lỗi database server. Đếm thất bại", None

def get_near_by_properties(client: MongoClient, ward, district, province, type, limit=8):
    try:
        status, message, data = find_properties(client=client, filter={"property_ward": ward, "property_district": district,
                                                                       "property_province": province, "property_type": type},
                                                sort={"property_linux": -1}, limit=limit)
        size = len(data)
        if size<limit:
            status, message, data_add = find_properties(client=client,
                                                     filter={"property_district": district, "property_province": province,
                                                             "property_type": type},
                                                     sort={"property_linux": -1}, limit=limit-size)
            data = data + data_add
            size = len(data)
            if size<limit:
                status, message, data_add = find_properties(client=client,
                                                            filter={"province": province, "type": type},
                                                            sort={"property_linux": -1}, limit=limit - size)
                data = data + data_add
        size = len(data)
        if size>0:
            return True, "Truy vấn tin bài thành công", data
        else:
            return False, "Không tìm thấy bất động sản gần đó", data
    except Exception as ex:
        print("Exception in get near by items from database", ex)
        return False, "Lỗi database server. Truy vấn tin bài thất bại", None

def get_list_ids(client: MongoClient, filter = {}):
    try:
        db = client.get_database("PropertiesDatabase")
        collection = db.get_collection("MediatedCleanData")
        data = collection.find(filter, {"_id": 1}).sort([("property_linux", -1)]).limit(100)
        data = list(data)
        result = []
        for x in data:
            result.append(x["_id"])
        return result
    except Exception as ex:
        print("Exception in read list ids", ex)
        return []

def get_items_by_ids(client: MongoClient, list_ids):
    try:
        db = client.get_database("PropertiesDatabase")
        collection = db.get_collection("MediatedCleanData")
        data = collection.find(filter={"_id": {"$in": list_ids}})
        data = list(data)
        for x in data:
            x["_id"] = str(x["_id"])
        return data
    except Exception as ex:
        print("Exception in get items by ids", ex)
        return []

def get_recommend_properties(user_client: MongoClient, item_client: MongoClient, userId, limit=15):
    try:
        db = user_client.get_database("UserDatabase")
        collection = db.get_collection("Preference")
        preference_obj = collection.find_one(filter={"userId": userId})
        if preference_obj is None:
            return get_random_properties(client=item_client, limit=limit)
        else:
            preference = preference_obj["preference"]
            filter = {}
            filter["property_ward"] = {"$in": preference["property_ward"]}
            filter["property_district"] = {"$in": preference["property_district"]}
            filter["property_type"] = {"$in": preference["property_type"]}
            filter["property_price"] = {"$gt": preference["min_price"], "$lt": preference["max_price"]}
            filter["property_area"] = {"$gt": preference["min_area"], "$lt": preference["max_area"]}
            list_ids = get_list_ids(client=item_client, filter=filter)
            if len(list_ids) < 50:
                filter.pop("property_ward")
                list_ids = list_ids + get_list_ids(client=item_client, filter=filter)
            if len(list_ids) < limit:
                random.shuffle(list_ids)
                list_ids = list_ids[:limit]
                data = get_items_by_ids(client=item_client, list_ids=list_ids)
                append_size = limit - len(data)
                status, message, append_data = get_random_properties(client=item_client, limit=append_size)
                if status is False:
                    return status, message, append_data
                else:
                    data = data + append_data
                    random.shuffle(data)
                    return True, "Lấy thông tin thành công", data
            else:
                random.shuffle(list_ids)
                list_ids = list_ids[:limit]
                data = get_items_by_ids(client=item_client, list_ids=list_ids)
                random.shuffle(data)
                return True, "Lấy thông tin thành công", data
    except:
        return False, "Lỗi database server. Truy vấn thất bại", None

def save_file_to_local(file_obj : FileStorage):
    try:
        new_file_name = str(uuid.uuid4()) + file_obj.filename
        save_path = os.path.join("temporary", new_file_name)
        file_obj.save(save_path)
        return save_path
    except Exception as ex:
        print("Exception in saving local file", ex)
        return None

def save_file_to_drive(drive : GoogleDrive, targetimagesavedir, file_obj : FileStorage):
    local_save_path = save_file_to_local(file_obj)
    if local_save_path is None:
        return None
    try:
        gfile = drive.CreateFile({'parents': [{'id': targetimagesavedir}], 'title': os.path.basename(local_save_path)})
        gfile.SetContentFile(local_save_path)
        gfile.Upload()
        id = gfile.metadata.get("id")
        del gfile
        return "https://drive.google.com/uc?export=view&id="+id
    except Exception as ex:
        del gfile
        print("Exception in save file in drive", ex)
        return None
    finally:
        os.remove(local_save_path)

def add_new_property(drive: GoogleDrive, targetimagesavedir,
                     client: MongoClient, user_id,
                     title, detail, district, ward, street, prop_type,
                     price, area, images):
    if title=="" or detail=="" or district=="" or ward=="" or street=="" or prop_type=="" or price<=0 or area<=0 or len(images)==0:
        return False, "Thông tin cung cấp không đầy đủ"
    address = street + ", " + ward + ", " + district + ", Hà Nội"
    property_images = []
    for image in images:
        x = save_file_to_drive(drive=drive, targetimagesavedir=targetimagesavedir, file_obj=image)
        if x is not None:
            property_images.append(x)
    data = {"property_title": title,
            "property_detail": detail,
            "property_ward": ward,
            "property_district": district,
            "property_province": "Thành phố Hà Nội",
            "property_type": prop_type,
            "property_price": price,
            "property_area": area,
            "property_address": address,
            "property_date": get_current_time_short_str(),
            "property_link": "USER"+user_id,
            "property_images": property_images,
            "property_linux": get_current_timestamp(),
            "property_search": detail+". "+address}
    try:
        db = client.get_database("UserDatabase")
        collection = db.get_collection("UserPost")
        collection.insert_one({"userId": user_id, "post": data})
        return True, "Tạo bài thành công"
    except:
        return False, "Lỗi database server, tạo bài thất bại"
