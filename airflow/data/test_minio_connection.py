from minio import Minio
from minio.error import S3Error

def test_minio_connection(minio_url, access_key, secret_key, bucket_name):
    # try:
        # Initialize the MinIO client
        minio_client = Minio(
            minio_url,
            access_key='wcNt85aVYrEn57uMT238',
            secret_key=secret_key,
            secure=False  # Set to True if using HTTPS
        )

        # Check if the bucket exists
        if minio_client.bucket_exists(bucket_name):
            print(f"Connection successful: Bucket '{bucket_name}' exists.")
        else:
            print(f"Bucket '{bucket_name}' does not exist. Creating bucket...")
            minio_client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
            
        # Upload a test file
        # test_file_content = b'This is a test file for MinIO connection.'
        # test_file_name = 'test_file.txt'
        # minio_client.put_object(
        #     bucket_name, 
        #     test_file_name, 
        #     data=io.BytesIO(test_file_content), 
        #     length=len(test_file_content), 
        #     content_type='text/plain'
        # )
        # print(f"Test file '{test_file_name}' uploaded successfully.")

        # # Download the test file
        # response = minio_client.get_object(bucket_name, test_file_name)
        # downloaded_content = response.read()
        # response.close()
        # response.release_conn()
        
        # print(f"Test file '{test_file_name}' downloaded successfully with content: {downloaded_content.decode('utf-8')}")

        # # Clean up by deleting the test file
        # minio_client.remove_object(bucket_name, test_file_name)
        # print(f"Test file '{test_file_name}' deleted successfully.")

    # except S3Error as e:
    #     print(f"S3Error: {e}")
    # except Exception as e:
    #     print(f"Error: {e}")

if __name__ == "__main__":
    MINIO_URL = 'minio:9000'  # Replace with your MinIO server URL
    # MINIO_URL = 'localhost:9001'  # Replace with your MinIO server URL
    ACCESS_KEY = 'wcNt85aVYrEn57uMT238'  # Replace with your MinIO access keya
    SECRET_KEY = 'hAPK9OfnhipSgP8iTclQvLjmOrhvajm0TUHZunqU'  # Replace with your MinIO secret key
    BUCKET_NAME = 'crawled-data'  # Replace with the name of your bucket

    test_minio_connection(MINIO_URL, ACCESS_KEY, SECRET_KEY, BUCKET_NAME)