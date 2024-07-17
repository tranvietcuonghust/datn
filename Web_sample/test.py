from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

upload_file_list = ['static/img/banner-ads.jpg']
targetDirID = "1cwZ6StLXH8BTQV-gH5L2tM1REvB3ftpk"
exist_file_list = drive.ListFile({'q': "'{}' in parents and trashed=false".format(targetDirID)}).GetList()

for upload_file in upload_file_list:
    if (not os.path.exists(upload_file)):
        continue

    fileName = os.path.basename(upload_file)
    for file1 in exist_file_list:
        if file1['title'] == fileName:
            file1.Delete()

    gfile = drive.CreateFile({'parents': [{'id': targetDirID}], 'title': fileName})
    # Read file and set it as the content of this instance.
    gfile.SetContentFile(upload_file)
    gfile.Upload()
    print(gfile.metadata.get("id"))
