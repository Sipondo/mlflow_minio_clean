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

for experiment in client.list_objects(BUCKET_NAME):
    print(f"Experiment: {experiment.object_name}")

    for run in client.list_objects(BUCKET_NAME, prefix=experiment.object_name):
        # print(f"Run: {run.object_name}")

        for file in client.list_objects(
            BUCKET_NAME, prefix=run.object_name + "artifacts/model"
        ):
            if file.object_name == run.object_name + "artifacts/model/":
                print(f"Removing folder: {file.object_name}")
                # print(client.remove_object(BUCKET_NAME, file.object_name))

                delete_object_list = map(
                    lambda x: DeleteObject(x.object_name),
                    client.list_objects(BUCKET_NAME, file.object_name, recursive=True),
                )
                errors = client.remove_objects(BUCKET_NAME, delete_object_list)
                for error in errors:
                    print("error occurred when deleting object", error)
                    raise Exception(errors)
