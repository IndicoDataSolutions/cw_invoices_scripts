import pandas as pd
import os

from indico import IndicoClient, IndicoConfig
from solutions_toolkit.snapshots.snapshot import Snapshot


def read_export(export_path, dataset_id):
    export_df = pd.read_csv(export_path)
    # assumes the dataset id will always be at the end of the column name
    export_df.rename(lambda x: x.replace(f"_{dataset_id}", ""), axis="columns", inplace=True)
    final_columns = [f"row_index", f"file_name", "document"]
    return export_df[final_columns]


HOST = os.getenv("INDICO_API_HOST", "cush.indico.domains")

# NOTE, Configure
API_TOKEN_PATH = "/home/fitz/Documents/customers/cushman-wakefield/indico_api_token.txt"
with open(API_TOKEN_PATH) as f:
    API_TOKEN = f.read()

my_config = IndicoConfig(host=HOST, api_token=API_TOKEN, verify_ssl=False)
INDICO_CLIENT = IndicoClient(config=my_config)

DATASET_ID = 100
WORKFLOW_ID = 187
MODEL_NAME = "Yardi Bank Rec Merged 01-23-21 v1.1 q147 model"

origial_snapshot_filepath = "/home/fitz/Documents/customers/cushman-wakefield/yardi-bank-rec/data/stack_review_data_02-02-021/merge_v1.csv"
original_snapshot = Snapshot.from_csv(origial_snapshot_filepath, text_col="document")

# note maybe we get this export in this script? maybe make it a indico_wrapper call?
dataset_export_filename = "/home/fitz/Documents/customers/cushman-wakefield/yardi-bank-rec/data/stack_review_data_02-02-021/dataset_export.csv"
dataset_export_df = read_export(dataset_export_filename, DATASET_ID)

review_snapshot = Snapshot.from_review_queue(INDICO_CLIENT, dataset_export_df, MODEL_NAME, WORKFLOW_ID, label_col="question", text_col="document")
stacked_snapshot = Snapshot.stack([original_snapshot, review_snapshot])
print("here")