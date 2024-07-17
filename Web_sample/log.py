from pymongo import MongoClient
from datetime import datetime
import pytz

timezone = pytz.timezone("Asia/Ho_Chi_Minh")

def get_current_time_str():
    current_time = datetime.now(tz=timezone)
    return current_time.strftime("%Y/%m/%d %H:%M:%S")

def get_current_time_short_str():
    current_time = datetime.now(tz=timezone)
    return current_time.strftime("%d/%m/%Y")

def get_current_timestamp():
    current_time = datetime.now(tz=timezone)
    return int(datetime.timestamp(current_time))

def write_log(client: MongoClient, user_id, item_id):
    try:
        db = client.get_database("UserDatabase")
        collection = db.get_collection("Log")
        current_time = get_current_time_str()
        collection.insert_one({"userId": user_id, "itemId": item_id, "timestamp": current_time})
        return True, "Ghi log thành công"
    except:
        return False, "Lỗi database server, ghi log thất bại"

def update_preference(client: MongoClient, user_id, item):
    try:
        db = client.get_database("UserDatabase")
        collection = db.get_collection("Preference")
        preference_obj = collection.find_one(filter={"userId": user_id})
        if preference_obj is not None:
            preference = preference_obj["preference"]
            if item["property_district"] not in preference["property_district"]:
                preference["property_district"].append(item["property_district"])
            if item["property_ward"] not in preference["property_ward"]:
                preference["property_ward"].append(item["property_ward"])
            if item["property_type"] not in preference["property_type"]:
                preference["property_type"].append(item["property_type"])
            if int(0.5*item["property_price"]) < preference["min_price"]:
                preference["min_price"] = int(0.5*item["property_price"])
            if int(1.5*item["property_price"]) > preference["max_price"]:
                preference["max_price"] = int(1.5*item["property_price"])
            if int(0.5*item["property_area"]) < preference["min_area"]:
                preference["min_area"] = int(0.5*item["property_area"])
            if int(1.5*item["property_area"]) > preference["max_area"]:
                preference["max_area"] = int(1.5*item["property_area"])
            collection.update_one(filter={"_id": preference_obj["_id"]}, update={"$set": {"preference": preference}})
        else:
            preference = {
                            "property_district": [item["property_district"]],
                            "property_ward": [item["property_ward"]],
                            "property_type": [item["property_type"]],
                            "min_price": int(0.5*item["property_price"]),
                            "max_price": int(1.5*item["property_price"]),
                            "min_area": int(0.5*item["property_area"]),
                            "max_area": int(1.5*item["property_area"])
                        }
            collection.insert_one(document={"userId": user_id, "preference": preference})
        return True, "Cập nhật sở thích thành công"
    except:
        return False, "Lỗi database server, cập nhật sở thích thất bại"