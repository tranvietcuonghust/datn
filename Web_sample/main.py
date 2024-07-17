from flask import Flask, jsonify, session, url_for, render_template, request, redirect, make_response
from pymongo import MongoClient
from authencation import *
from item import *
import json
from bson.objectid import ObjectId
from format import Format
from address import *
from log import *
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from statistic import *
gauth = GoogleAuth()
drive = GoogleDrive(gauth)
TARGETIMAGEDIRID = "1cwZ6StLXH8BTQV-gH5L2tM1REvB3ftpk"
from chatbot import  *
import mongomock
from google.cloud import bigquery
from google.oauth2 import service_account
formater = Format()
import hashlib
import base64
import json

app = Flask(__name__)
app.secret_key = 'BATDONGSANHANOI'



# USER_CLIENT = MongoClient("mongodb+srv://nguyenvannga1507:nguyenvannga1507@cluster0.26pvh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
# ITEM_CLIENT = MongoClient("mongodb+srv://nguyenvannga1507:nguyenvannga1507@cluster0.fpk9o.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
# ADDRESS_CLIENT = MongoClient("mongodb+srv://nguyenvannga1507:nguyenvannga1507@cluster0.8ow38.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
# STATISTICAL_CLIENT =  MongoClient("mongodb+srv://nguyenvannga1507:nguyenvannga1507@cluster0.mtinl.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

USER_CLIENT = mongomock.MongoClient()
ITEM_CLIENT = mongomock.MongoClient()
ADDRESS_CLIENT = mongomock.MongoClient()
STATISTICAL_CLIENT = mongomock.MongoClient()

key_path = "./bds-warehouse-424208-fca17cf1fff7.json"

# Load credentials from the service account key file
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials,project="bds-warehouse-424208")


def sanitize_data(data):
    for item in data:
        for key, value in item.items():
            if value != value:  # NaN check: NaN is not equal to itself
                item[key] = None
    return data
def url_to_id(url):
    return base64.urlsafe_b64encode(url.encode()).decode()
def id_to_url(id):
    # Decode the Base64 back into bytes and then to a string
    return base64.urlsafe_b64decode(id.encode()).decode()
@app.route('/', methods=["GET"])
def home():
    # status, message, data = get_random_properties(client=ITEM_CLIENT)
    # return render_template("index.html", random_data=data, formater=formater)
    query_job = client.query("""
        SELECT * FROM `bds-warehouse-424208.bds.bds_3`
        ORDER BY RAND()
        LIMIT 10
    """)

    results = query_job.result()  # Waits for job to complete.
    

    data = [dict(row) for row in results]
    for item in data:
        item['URL'] = url_to_id(item['URL'])

    return render_template("index.html", random_data=data, formater=formater)

@app.route('/login', methods=["GET"])
def login_get():
    email = request.cookies.get('email', "")
    password = request.cookies.get("password", "")
    return render_template("signin.html", email=email, password=password)

@app.route('/login', methods=["POST"])
def login_post():
    email = request.form.get("email", "")
    password = request.form.get("password", "")
    save = request.form.get("save", "")
    status, message, account = login(client=USER_CLIENT, email=email, password=password)
    if status:
        account["_id"] = str(account["_id"])
        session["user"] = account
        if save == "on":
            res = make_response(redirect(url_for("home")))
            res.set_cookie("email", email)
            res.set_cookie("password", password)
            return res
        else:
            return redirect(url_for("home"))
    else:
        return render_template("signin.html", message=message, email=email, password=password)

@app.route('/signup', methods=["GET"])
def signup_get():
    return render_template("register.html")

@app.route('/signup', methods=["POST"])
def signup_post():
    user_name = request.form.get('name', "")
    email = request.form.get("email", "")
    password = request.form.get("password", "")
    password_confirm = request.form.get("confirm-password", "")
    status, message = create_account(client=USER_CLIENT, user_name=user_name, email=email, password=password, password_confirm=password_confirm)
    if status:
        return redirect(url_for("login_get"))
    else:
        return render_template("register.html", message=message, user_name=user_name, email=email, password=password, password_confirm=password_confirm)

@app.route('/logout', methods=["GET"])
def logout_get():
    if session.get("user"):
        session.pop("user")
    return redirect(url_for("home"))

@app.route('/search', methods=["GET"])
def search_get():
    query = request.args.get("query", "")
    query = request.form.get("query", "{}")
    city = request.args.get("city", "")
    district = request.args.get("district", "")
    ward = request.args.get("ward", "")
   
    filter= {}
    # filter = query.get("filter", {})
    if city and city != "Tinh/thanh pho":
        filter["City"] = city
    if district and district != "Quan/huyen":
        filter["District"] = district
    if ward and ward != "Phuong/xa":
        filter["Ward"] = ward
    print("------------------",filter)
    return render_template("properties.html", query=query, filter = filter)

@app.route('/find_items', methods=["POST"])
def find_items_post():
    query = request.form.get("query", "{}")
    
    print(request.form)
    query = json.loads(query)
    limit = query.get('limit', 15)
    offset = query.get("offset", 0)
    filter = query.get("filter", {})
    sort = query.get("sort", {"property_linux": -1})
    print(filter)
    status, message, data = find_properties(client=client, filter=filter,
                                            sort=sort, offset=offset, limit=limit)
    
    if len(data)>0:
        for item in data:
            item['URL'] = url_to_id(item['URL'])
    data = sanitize_data(data)
    if status == False:
        data = []
     # Check if the request is AJAX
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        
        return jsonify({'status': status, 'message': message, 'data': data, 'filter': filter})
    else:
        print(data)
        return render_template('properties.html', status=status, message=message, data=data, formater=formater, filter=filter)

@app.route('/count_items', methods=["POST"])
def count_items_post():
    query = request.form.get("query", "{}")
    query = json.loads(query)
    filter = query.get("filter", {})
    status, message, data = count_properties(client=ITEM_CLIENT, filter=filter)
    return {"status": status, "message": message, "data": data}

@app.route('/item_detail', methods=["GET"])
def item_detail_get():
    id = request.args.get("id")
    # try:
        
    # id = ObjectId(id)
    id = id_to_url(id)
    print(id)
    status, message, item = get_property(client=client, id=id)
    if status:
        return render_template("properties-detail.html", item=item, formater=formater)
    else:
        print("message", message)
        return render_template("404.html")
    # except Exception as ex:
    #     print(ex)
    #     return render_template("404.html")

@app.route('/dashboard1')
def dashboard1():
    return render_template('dashboard.html')

@app.route('/dashboard2')
def dashboard2():
    return render_template('dashboard_city.html')

@app.route('/get_districts', methods=["POST"])
def get_districts_post():
    try:
        province = request.form.get("province",0)
        # province = "Hà Nội"
        status, message, data = get_list_districts(client=client, province_id=province)
        return {"status": status, "message": message, "data": data}
    except:
        return {"status": False, "message": "Sai tham số", "data": []}
    
@app.route('/get_cities', methods=["POST"])
def get_cities_post():
    try:
        province = request.form.get("province", 1)
        # province = ""
        status, message, data = get_list_cities(client=client)
        return {"status": status, "message": message, "data": data}
    except:
        return {"status": False, "message": "Sai tham số", "data": []}

@app.route('/get_wards', methods=["POST"])
def get_wards_post():
    try:
        district = request.form.get("district")
        district = district
        print(">>>>>>>>>>",district)
        status, message, data = get_list_wards(client=client, district_id=district)
        return {"status": status, "message": message, "data": data}
    except:
        return {"status": False, "message": "Sai tham số", "data": []}

@app.route('/write_log', methods=["POST"])
def write_log_post():
    user_id = request.form.get("userId", "")
    item_id = request.form.get("itemId", "")
    try:
        item_id = ObjectId(item_id)
        status, message, data = get_property(client=ITEM_CLIENT, id=item_id)
        if status is False:
            return {"status": False, "message": "Xử lý log thất bại"}
        status, message = write_log(client=USER_CLIENT, user_id=user_id, item_id=data["_id"])
        if status is False:
            return {"status": False, "message": "Xử lý log thất bại"}
        status, message = update_preference(client=USER_CLIENT, user_id=user_id, item=data)
        if status is False:
            return {"status": False, "message": "Xử lý log thất bại"}
        else:
            return {"status": True, "message": "Xử lý log thành công"}
    except:
        return {"status": False, "message": "Id tin bài không tồn tại"}

@app.route('/get_recommend', methods=["POST"])
def get_recommend_post():
    user_id = request.form.get("userId", "")
    status, message, data = get_recommend_properties(user_client=USER_CLIENT, item_client=ITEM_CLIENT, userId=user_id)
    if data is None:
        data = []
    return {"status": status, "message": message, "data": data}

@app.route('/create_property', methods=["GET"])
def create_property_get():
    return render_template("create_item.html")

@app.route('/create_property', methods=["POST"])
def create_property_post():
    if session.get("user"):
        print(request.form)
        title = request.form.get("title", "")
        detail = request.form.get("detail", "")
        prop_type = request.form.get("type", "")
        district = request.form.get("district", "")
        district = find_district(client=ADDRESS_CLIENT, district_id=district)
        ward = request.form.get("ward", "")
        ward = find_ward(client=ADDRESS_CLIENT, ward_id=ward)
        street = request.form.get("street", "")
        try:
            price = request.form.get("price", "")
            area = request.form.get("area", "")
            price = int(price)
            area = int(area)
        except:
            price = 0
            area = 0
        list_images = request.files.getlist("images")
        status, message = add_new_property(drive=drive, targetimagesavedir=TARGETIMAGEDIRID,
                                           client=USER_CLIENT, user_id = session.get("user")["_id"],
                                            title=title, prop_type=prop_type, detail=detail, district=district, ward=ward,
                                           street=street, price=price, area=area, images=list_images)
        return "<h1>Create post successfully </h1>"
    else:
        return "<h1>Fail to create post </h1>"

@app.route('/statistic', methods=["GET"])
def statistic_post():
    is_post = request.args.get("post")
    if is_post == "true":
        is_post = True
    else:
        is_post = False
    return render_template("dashboard.html", is_post=is_post)

@app.route('/get_statistic', methods=["POST"])
def get_statistic_post():
    district = request.form.get("district")
    ward = request.form.get("ward")
    pro_type = request.form.get("type")
    is_post = request.form.get("post")
    day = int(request.form.get("day"))
    area_type = request.form.get("area-type")
    price_type = request.form.get("price-type")
    if is_post == "true":
        is_post = True
    else:
        is_post = False
    status, message, data = get_statistic_data(STATISTICAL_CLIENT=STATISTICAL_CLIENT, prop_type=pro_type,
                              district=district, ward=ward, is_post=is_post, day=day,
                                               area_type=area_type, price_type=price_type)
    return {"status": status, "message": message, "data": data}

@app.route('/get_sub_type', methods=["POST"])
def get_sub_type_post():
    pro_type = request.form.get("type")
    return {"area_type": AREA_TYPE_MAPPING[pro_type], "price_type": PRICE_TYPE_MAPPING[pro_type]}

@app.route('/send_password', methods=["GET"])
def send_password_get():
    return render_template("send_password.html")

@app.route('/send_password', methods=["POST"])
def send_password_post():
    email = request.form.get("email")
    status, message = send_password(client=USER_CLIENT, email=email)
    return render_template("send_password.html", email=email, message=message)

@app.route('/chatbot', methods=["GET"])
def chatbot_get():
    return render_template("chatbot.html")

@app.route('/chatbot', methods=["POST"])
def chatbot_post():
    query = request.form.get("query", "")
    result = get_chatbot_result(ITEM_CLIENT=ITEM_CLIENT, STATISTICAL_CLIENT=STATISTICAL_CLIENT, query=query)
    return result

if __name__ == '__main__':
    app.run(debug=True, port=5002)

