from minio import Minio
from minio.deleteobjects import DeleteObject

with open("keys") as infile:
    keys = [x.strip() for x in infile.readlines()]

client = Minio(
    "130.226.140.28:5000", access_key=keys[0], secret_key=keys[1], secure=False
)


BUCKET_NAME = "cats-storage"
if not client.bucket_exists(BUCKET_NAME):
    raise Exception("Bucket not found!")

superlist = []
total_size = 0

for x in client.list_objects(BUCKET_NAME, "5/", recursive=True):
    total_size += x.size
    # print(
    #     f"{total_size/1000000000}GB : {total_size/1000000}MB : {total_size/1000}KB : {total_size}B"
    # )
    print(f"{total_size/1000000000}GB : {x.object_name}")
    superlist.append((x.object_name, x.size / 1000000000))

superlist.sort(key=lambda x: -x[1])

superlist
