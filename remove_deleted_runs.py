from minio import Minio
from minio.deleteobjects import DeleteObject


import os

with open("keys") as infile:
    keys = [x.strip() for x in infile.readlines()]

os.environ["MLFLOW_TRACKING_USERNAME"] = keys[2]
os.environ["MLFLOW_TRACKING_PASSWORD"] = keys[3]


from mlflow import MlflowClient, set_tracking_uri
from mlflow.entities import ViewType

client = Minio(
    "130.226.140.28:5000", access_key=keys[0], secret_key=keys[1], secure=False
)
BUCKET_NAME = "cats-storage"

if not client.bucket_exists(BUCKET_NAME):
    raise Exception("Bucket not found!")

mlflow = MlflowClient("https://res42.itu.dk")

experiments = sorted(
    [x.experiment_id for x in mlflow.search_experiments()], key=lambda x: int(x)
)

runs = mlflow.search_runs(experiments, run_view_type=ViewType.DELETED_ONLY)

for run in runs:
    if run.info.lifecycle_stage != "deleted":
        raise Exception(f"Non-deleted run found! {run}")

for run in runs:
    if run.info.lifecycle_stage != "deleted":
        raise Exception(f"Non-deleted run found! {run}")

    delete_object_list = map(
        lambda x: DeleteObject(x.object_name),
        client.list_objects(
            BUCKET_NAME,
            prefix=f"{run.info.experiment_id}/{run.info.run_id}/",
            recursive=True,
        ),
    )

    if list(delete_object_list):
        print(f"Removing run: {run.info.run_name} : {run.info.run_id}")

        errors = client.remove_objects(BUCKET_NAME, delete_object_list)
        for error in errors:
            print("error occurred when deleting object", error)
            raise Exception(errors)
