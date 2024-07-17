from google.cloud import dialogflow
import os
from proto.marshal.collections.repeated import RepeatedComposite
from proto.marshal.collections.maps import MapComposite
from pymongo import MongoClient
from format import Format
import datetime
import pandas as pd
import numpy as np

DAY_TO_ANALYSIZE = 30
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"realestate-331714-323b6d382eaa.json"
PROJECT_ID = 'realestate-331714'

LIST_PROPERTY_TYPE = ["nhà mặt tiền", "nhà trong hẻm", "căn hộ chung cư", "đất nền",
                      "biệt thự", "khách sạn, cửa hàng", "phòng trọ, nhà trọ",
                      "văn phòng", "loại hình khác"]
def get_date_limit(parameters):
    if isinstance(parameters["time_data"], dict):
        value = parameters["time_data"]["about"]
        unit = parameters["time_data"]["time_type"]
        if unit=="ngày":
            return value
        if unit=="tháng":
            return value*30
        if unit=="năm":
            return value*365
        return DAY_TO_ANALYSIZE
    else:
        return DAY_TO_ANALYSIZE

def get_intent(session_id, text, project_id=PROJECT_ID, language_code='vi'):
    session_client = dialogflow.SessionsClient()

    session = session_client.session_path(project_id, session_id)

    text_input = dialogflow.TextInput(text=text, language_code=language_code)

    query_input = dialogflow.QueryInput(text=text_input)

    response = session_client.detect_intent(
        request={"session": session, "query_input": query_input}
    )

    intent = response.query_result.intent.display_name

    parameters = dict(response.query_result.parameters)

    for key, value in parameters.items():
        if type(value) not in [str, float, int]:
            try:
                if isinstance(value, RepeatedComposite):
                    parameters[key] = list(value)
                elif isinstance(value, MapComposite):
                    parameters[key] = dict(value)
            except:
                print("Exception occur", key, parameters[key], type(parameters[key]))

    context_list = []
    for i, context in enumerate(response.query_result.output_contexts):
        context_parameters = dict(context.parameters)
        for key, value in context_parameters.items():
            if type(value) not in [str, float, int, list]:
                try:
                    if isinstance(value, RepeatedComposite):
                        context_parameters[key] = list(value)
                    elif isinstance(value, MapComposite):
                        context_parameters[key] = dict(value)
                except:
                    print("Exception occur", key, context_parameters[key], type(context_parameters[key]))
        context_list.append(context_parameters)

    return intent, parameters, context_list

def get_location_data_filter(filter, parameters):
    if isinstance(parameters["location_data"], dict):
        location_data = parameters["location_data"]
        if "province" in location_data:
            filter["property_province"] = location_data["province"]
        if "district" in location_data:
            filter["property_district"] = location_data["district"]
        if "ward" in location_data:
            filter["property_ward"] = location_data["ward"]

def get_property_type_filter(filter, parameters):
    if isinstance(parameters["property_type"], list):
        property_type_data = parameters["property_type"]
        property_type_data = [item for item in property_type_data if item in LIST_PROPERTY_TYPE]
        if len(property_type_data) != 0:
            filter["property_type"] = {"$in": property_type_data}

def get_property_area_filter(filter, parameters):
    if isinstance(parameters["area_data"], dict):
        area_data = parameters["area_data"]
        if "about" in area_data:
            value = area_data["about"]
            if "area_unit" in area_data:
                if area_data["area_unit"] == "ha":
                    value = value*1000
            filter["property_area"] = {"$gt": value*0.9, "$lt": value*1.1}
            return

        if "from" in area_data and "to" in area_data:
            from_value = area_data["from"]
            to_value = area_data["to"]
            if "area_unit" in area_data:
                area_unit = area_data["area_unit"]
                if isinstance(area_unit, list):
                    if len(area_unit) == 2:
                        if area_unit[0] == "ha":
                            from_value = from_value*1000
                        if area_unit[1] == "ha":
                            to_value = to_value*1000
                    elif len(area_unit) == 1:
                        if area_unit[0] == "ha":
                            from_value = from_value * 1000
                            to_value = to_value * 1000
                elif area_unit == "ha":
                    from_value = from_value*1000
                    to_value = to_value*1000
            filter["property_area"] = {"$gt": from_value, "$lt": to_value}
            return

        if "from" in area_data:
            from_value = area_data["from"]
            if "area_unit" in area_data:
                if area_data["area_unit"] == "ha":
                    from_value = from_value*1000
            filter["property_area"] = {"$gt": from_value}
            return

        if "to" in area_data:
            to_value = area_data["to"]
            if "area_unit" in area_data:
                if area_data["area_unit"] == "ha":
                    to_value = to_value*1000
            filter["property_area"] = {"$lt": to_value}
            return

def get_property_price_filter(filter, parameters):
    if isinstance(parameters["price_data"], dict):
        price_data = parameters["price_data"]
        if "about" in price_data:
            value = price_data["about"]
            if "price_unit" in price_data:
                if price_data["price_unit"] == "triệu":
                    value = value*1000000
                if price_data["price_unit"] == "tỷ":
                    value = value*1000000000
            else:
                value = value * 1000000000
            filter["property_price"] = {"$gt": value*0.9, "$lt": value*1.1}
            return

        if "from" in price_data and "to" in price_data:
            from_value = price_data["from"]
            to_value = price_data["to"]
            if "price_unit" in price_data:
                price_unit = price_data["price_unit"]
                if isinstance(price_unit, list):
                    if len(price_unit) == 2:
                        if price_unit[0] == "triệu":
                            from_value = from_value*1000000
                        else:
                            from_value = from_value*1000000000
                        if price_unit[1] == "triệu":
                            to_value = to_value*1000000
                        else:
                            to_value = to_value*1000000000
                    elif len(price_unit) == 1:
                        if price_unit[0] == "triệu":
                            from_value = from_value * 1000000
                            to_value = to_value * 1000000
                        else:
                            from_value = from_value * 1000000000
                            to_value = to_value * 1000000000
                elif price_unit == "triệu":
                    from_value = from_value*1000000
                    to_value = to_value*1000000
                else:
                    from_value = from_value * 1000000000
                    to_value = to_value * 1000000000
            filter["property_price"] = {"$gt": from_value, "$lt": to_value}
            return

        if "from" in price_data:
            from_value = price_data["from"]
            if "price_unit" in price_data:
                if price_data["price_unit"] == "triệu":
                    from_value = from_value*1000000
                else:
                    from_value = from_value*1000000000
            else:
                from_value = from_value * 1000000000
            filter["property_price"] = {"$gt": from_value}
            return

        if "to" in price_data:
            to_value = price_data["to"]
            if "price_unit" in price_data:
                if price_data["price_unit"] == "triệu":
                    to_value = to_value*1000000
                else:
                    to_value = to_value*1000000000
            else:
                to_value = to_value * 1000000000
            filter["property_price"] = {"$lt": to_value}
            return

def get_list_estate_data(ITEM_CLIENT: MongoClient, parameters):
    min_time = int(datetime.datetime.strptime("16/12/21 00:00:00", "%d/%m/%y %H:%M:%S").timestamp()) - get_date_limit(parameters) * 86400
    filter = {"property_linux": {"$gt": min_time}}
    get_location_data_filter(filter, parameters)
    get_property_area_filter(filter, parameters)
    get_property_type_filter(filter, parameters)
    get_property_price_filter(filter, parameters)

    db = ITEM_CLIENT.get_database("PropertiesDatabase")
    collection = db.get_collection("MediatedCleanData")

    data = collection.find(filter).limit(9)
    data = list(data)
    for x in data:
        x["_id"] = str(x["_id"])

    return data

def render_get_list_estate(ITEM_CLIENT: MongoClient, parameters):
    formater = Format()
    data = get_list_estate_data(ITEM_CLIENT=ITEM_CLIENT, parameters=parameters)
    if len(data) == 0:
        return "<div class='bot-answer'>Không có tin bài nào phù hợp!</div>"
    answer = "<div class='bot-answer'><h5 style='margin-bottom: 20px;'>DANH SÁCH TIN BÀI</h5>"
    list_item = "<div class='list-items-property'>"
    for item in data:
        add = formater.short_address(item["property_address"])
        price = formater.price_format(item["property_price"])
        area = formater.area_format(item["property_area"])
        if len(item["property_images"]):
            img = item["property_images"][0]
        else:
            img = "/static/img/slider/6.jpg"
        this_item = """<div class="item-property">
                            <div class="item-address">Địa chỉ: %s</div>
                            <div class="item-area">Giá: %s</div>
                            <div class="item-price">Diện tích: %s</div>
                            <div><a href="/item_detail?id=%s"><img src='%s'></a></div>
                       </div>""" % (add, price, area, item["_id"], img)
        list_item += this_item
    list_item += "</div>"
    answer = answer + list_item + "</div>"
    return answer


def get_price_data(ITEM_CLIENT: MongoClient, parameters):
    min_time = int(datetime.datetime.strptime("16/12/21 00:00:00", "%d/%m/%y %H:%M:%S").timestamp()) - get_date_limit(parameters) * 86400
    filter = {"property_linux": {"$gt": min_time}}
    get_location_data_filter(filter, parameters)
    get_property_area_filter(filter, parameters)
    get_property_type_filter(filter, parameters)

    db = ITEM_CLIENT.get_database("PropertiesDatabase")
    collection = db.get_collection("MediatedCleanData")

    data = collection.find(filter, {"property_date": 1, "property_linux": 1, "property_area": 1, "property_price": 1})
    data = list(data)
    if len(data) == 0:
        return None, None

    formater = Format()

    data = pd.DataFrame(data)
    data = data.groupby(["property_date", "property_linux"]).agg({"property_area": "sum",
                                                                  "property_price": "sum"})
    data["mean_price"] = data["property_price"]/data["property_area"]
    data["mean_price"] = data["mean_price"].astype(int)
    data["mean_price"] = data["mean_price"].apply(lambda x: formater.price_format(x))
    data.reset_index(inplace=True)
    data.sort_values("property_linux", ascending=True, inplace=True)

    mean_price = np.sum(data["property_price"])/np.sum(data["property_area"])
    mean_price = formater.price_format(mean_price)

    return mean_price, data

def render_price(ITEM_CLIENT: MongoClient, parameters):
    try:
        mean_price, data = get_price_data(ITEM_CLIENT=ITEM_CLIENT, parameters=parameters)
        if mean_price is None:
            return "<div class='bot-answer'>Không có dữ liệu phù hợp!</div>"
        content = "<div class='bot-answer'> <h5  style='margin-bottom: 30px;'>GIÁ TRUNG BÌNH: %s/m2 </h5>" % mean_price
        times = "<ul class='list-group'>"
        for idx, row in data.iterrows():
            this_time = "<li class='list-group-item' style='color: #51b2db; font-weight: bold;'>%s: %s/m2</li>" % (row["property_date"], row["mean_price"])
            times += this_time
        times = times + "</ul>"
        content = content + times + "</div>"
        return content
    except Exception as ex:
        print("Exception occur", ex)
        return "<div class='bot-answer'>Xin lỗi! Chúng tôi không thể kết nối với bạn!</div>"

def get_place_data(ITEM_CLIENT: MongoClient, parameters):
    min_time = int(datetime.datetime.strptime("16/12/21 00:00:00", "%d/%m/%y %H:%M:%S").timestamp()) - get_date_limit(parameters) * 86400
    filter = {"property_linux": {"$gt": min_time}}
    get_location_data_filter(filter, parameters)
    get_property_type_filter(filter, parameters)

    db = ITEM_CLIENT.get_database("PropertiesDatabase")
    collection = db.get_collection("MediatedCleanData")

    data = collection.find(filter, {"property_province": 1, "property_district": 1,
                                    "property_ward": 1, "property_price": 1, "property_area": 1})
    data = list(data)
    if len(data) == 0:
        return None, None, None

    data = pd.DataFrame(data)
    add_group = None
    ask_ward = False
    has_district = False

    if "property_district" in filter:
        has_district = True

    if parameters["location_type"] != "":
        if parameters["location_type"] == "province":
            add_group = ["property_province"]
        elif parameters["location_type"] == "district":
            add_group = ["property_district", "property_province"]
        elif parameters["location_type"] == "ward":
            ask_ward = True
            add_group = ["property_ward", "property_district", "property_province"]
    else:
        add_group = ["property_province", "property_district"]
        if "property_district" in filter:
            ask_ward = True
            add_group.append("property_ward")

    data = data.groupby(add_group).agg({"_id": "count",
                                        "property_price": "sum",
                                        "property_area": "sum"})
    data["mean_price"] = data["property_price"]/data["property_area"]
    data.reset_index(inplace=True)

    if ask_ward:
        if has_district:
            data["address"] = data["property_ward"]
        else:
            data["address"] = data.apply(lambda x: x["property_ward"]+", "+x["property_district"], axis=1)
    else:
        data["address"] = data["property_district"]

    ask_price = False

    if parameters["price_type"] != "":
        ask_price = True
        if parameters["price_type"] == "thấp":
            data.sort_values("mean_price", ascending=True, inplace=True)
        if parameters["price_type"] == "cao":
            data.sort_values("mean_price", ascending=False, inplace=True)
    else:
        if parameters["property_amount"] == "ít":
            data.sort_values("_id", ascending=True, inplace=True)
        else:
            data.sort_values("_id", ascending=False, inplace=True)

    if len(data)>10:
        data = data.iloc[:10]

    fomater = Format()
    data["mean_price"] = data["mean_price"].apply(lambda x: fomater.price_format(x))

    return data, ask_price, ask_ward

def render_place(ITEM_CLIENT: MongoClient, parameters):
    try:
        data, ask_price, ask_ward = get_place_data(ITEM_CLIENT=ITEM_CLIENT, parameters=parameters)
        if ask_ward:
            add = 'PHƯỜNG XÃ'
        else:
            add = 'QUẬN HUYỆN'
        if ask_price:
            if parameters["price_type"] == "thấp":
                content = "<div class='bot-answer'><h5 style='margin-bottom: 30px;'>NHỮNG %s CÓ GIÁ THẤP NHẤT</h5>" % add
            else:
                content = "<div class='bot-answer'><h5 style='margin-bottom: 30px;'>NHỮNG %s CÓ GIÁ CAO NHẤT</h5>" % add
            all_place = "<ul class='list-group'>"
            for idx, row in data.iterrows():
                this_place = "<li class='list-group-item' style='color: #51b2db; font-weight: bold;'>%s: %s/m2</li>" % (row["address"], row["mean_price"])
                all_place += this_place
            all_place += "</ul>"
            content = content + all_place + "</div>"
            return content
        else:
            if parameters["property_amount"] == "ít":
                content = "<div class='bot-answer'><h5 style='margin-bottom: 30px;'>NHỮNG %s CÓ ÍT TIN BÀI NHẤT</h5>" % add
            else:
                content = "<div class='bot-answer'><h5 style='margin-bottom: 30px;'>NHỮNG %s CÓ NHIỀU TIN BÀI NHẤT</h5>" % add
            all_place = "<ul class='list-group'>"
            for idx, row in data.iterrows():
                this_place = "<li class='list-group-item' style='color: #51b2db; font-weight: bold;'>%s: %s tin bài</li>" % (row["address"], row["_id"])
                all_place += this_place
            all_place += "</ul>"
            content = content + all_place + "</div>"
            return content
    except Exception as ex:
        print("Exception occur", ex)
        return "<div class='bot-answer'>Xin lỗi! Chúng tôi không thể kết nối với bạn!</div>"


def get_chatbot_result(ITEM_CLIENT: MongoClient, STATISTICAL_CLIENT: MongoClient, query):
    try:
        intent, parameters, context_list = get_intent('1', query)
        if intent == "Get List Estate Intent":
            answer = render_get_list_estate(ITEM_CLIENT=ITEM_CLIENT, parameters=parameters)
            return answer
        if intent == "Ask price Intent":
            answer = render_price(ITEM_CLIENT=ITEM_CLIENT,parameters=parameters)
            return answer
        if intent == "Ask Place Intent":
            answer = render_place(ITEM_CLIENT=ITEM_CLIENT, parameters=parameters)
            return answer
        if intent == "Ask name Intent":
            return "<div class='bot-answer'>Tôi là trợ lý ảo của Cổng thông tin bất động sản. Rất vui được giúp đỡ bạn!</div>"
    except Exception as ex:
        print("Exception occur", ex)
        return "<div class='bot-answer'>Xin lỗi! Chúng tôi không thể kết nối với bạn!</div>"


