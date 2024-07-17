from pymongo import MongoClient
import requests
import smtplib

def create_account(client : MongoClient, user_name, email, password, password_confirm):
    if user_name == "" or email == "" or password == "" or password_confirm == "":
        return False, "Hãy điền đầy đủ thông tin"
    if password != password_confirm:
        return False, "Mật khẩu xác nhận không đúng"
    try:
        db = client.get_database("UserDatabase")
        collection = db.get_collection("Account")
        account = collection.find_one(filter={"email": email})
        if account:
            return False, "Email đã được sử dụng"
        else:
            response = requests.get("https://isitarealemail.com/api/email/validate", params={'email': email})
            status = response.json()['status']
            if status == "valid":
                is_valid = True
            else:
                is_valid = False

            if is_valid:
                collection.insert_one(document={"username": user_name, "email": email, "password": password})
                return True, "Tạo tài khoản thành công"
            else:
                return False, "Email không tồn tại"
    except:
        return False, "Lỗi database server, tạo tài khoản thất bại"

def login(client : MongoClient, email, password):
    try:
        db = client.get_database("UserDatabase")
        collection = db.get_collection("Account")
        account = collection.find_one(filter={"email": email, "password": password})
        if account:
            return True, "Đăng nhập thành công", account
        else:
            return False, "Thông tin đăng nhập không đúng", None
    except:
        return False, "Lỗi database server, đăng nhập thất bại", None

def find_account_by_email(client: MongoClient, email):
    try:
        db = client.get_database("UserDatabase")
        collection = db.get_collection("Account")
        account = collection.find_one(filter={"email": email})
        if account:
            return True, "Tìm tài khoản thành công", account
        else:
            return False, "Tài khoản không tồn tại", None
    except:
        return False, "Lỗi database server, đăng nhập thất bại", None

def send_password(client: MongoClient, email):
    status, message, account = find_account_by_email(client=client, email=email)
    if status == False:
        return status, message
    else:
        gmail_user = 'marchchutavuth01011997@gmail.com'
        gmail_app_password = 'tuoigimadoanduoc'

        sent_from = gmail_user

        email = email
        password = account["password"]

        sent_subject = "Thông tin mật khẩu đăng nhập Cổng thông tin bất động sản"
        sent_body = "Thông tin đăng nhập của bạn\n\n     Email: %s \n     Password: %s \n\nTrân trọng!" % (
        email, password)

        email_text = "From: %s\nTo: %s\nSubject: %s\n%s" % (sent_from, email, sent_subject, sent_body)

        try:
            server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
            server.ehlo()
            server.login(gmail_user, gmail_app_password)
            server.sendmail(sent_from, email, email_text.encode("utf-8"))
            server.close()
            print('Email sent!')
        except Exception as exception:
            print("Error: %s!\n\n" % exception)

        return True, "Thông tin đăng nhập đã được gửi về email của bạn!"


