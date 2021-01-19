import os
import pandas as pd

from indico.queries import GraphQLRequest, GetDataset

from config import INDICO_CLIENT, COMPLETE_STATUS, DATASET_ID
from utils import read_export
from snapshot import Snapshot
from queries import CREATE_TEACH_TASK, GET_TEACH_TASK, SUBMIT_QUESTIONNAIRE_EXAMPLE

data_dir = "/home/fitz/Documents/customers/cushman-wakefield/va-invoices/add_reviewed_data_project/data/merge_01_02/"
snapshot_filename = os.path.join(data_dir, "teach_task_125.csv")
snapshot_1 = Snapshot.from_csv(snapshot_filename)

snapshot_filename = os.path.join(data_dir, "teach_task_93.csv")
snapshot_2 = Snapshot.from_csv(snapshot_filename)

snapshots = [snapshot_1, snapshot_2]
merged_snapshot = Snapshot.merge_snapshots(snapshots)
merged_df = merged_snapshot.to_df()

# TODO: Add logic for creating task
model_fields = merged_snapshot.get_label_list()
unlabeled_fields = [
    "Comm Slip: Name - Outside CW Part",
    "Comm Slip: Earnings - Outside CW Part",
]
dummy_fields = [
    "CM Slip: Execution & Billed Client validated?",
    "LOE: Execution & Billed Client validated?",
    "POD: Delivery per Client Agrmt. validated?",
    "Invoice: Accuracy & Billed Client validated?",
]
full_field_list = model_fields + dummy_fields + unlabeled_fields

# first create teach task
dataset = INDICO_CLIENT.call(GetDataset(DATASET_ID))
source_col_id = dataset.datacolumn_by_name("text").id
teach_task_name = "V&A Merged Teach Task V3.6"
variables = {
    "name": teach_task_name,
    "processors": [],
    "dataType": "TEXT",
    "datasetId": DATASET_ID,
    "numLabelersRequired": 1,
    "sourceColumnId": source_col_id,
    "questions": [
        {
            "type": "ANNOTATION",
            "targets": full_field_list,
            "keywords": [],
            "text": teach_task_name,
        }
    ],
}
teach_task = INDICO_CLIENT.call(
    GraphQLRequest(query=CREATE_TEACH_TASK, variables=variables)
)
teach_task_id = teach_task["createQuestionnaire"]["id"]

variables = {"id": teach_task_id}
teach_task_stats = INDICO_CLIENT.call(
    GraphQLRequest(query=GET_TEACH_TASK, variables=variables)
)
question_data = teach_task_stats["questionnaires"]["questionnaires"][0]["questions"][0]
labelset_id = question_data["labelset"]["id"]
model_group_id = question_data["modelGroupId"]

labels = []
for _, row in merged_df.iterrows():
    row_index = row[f"row_index_{DATASET_ID}"]
    target = row.question
    labels.append({"rowIndex": row_index, "target": target})

variables = {
    "datasetId": DATASET_ID,
    "labelsetId": labelset_id,
    "labels": labels,
    "modelGroupId": model_group_id,
}

INDICO_CLIENT.call(
    GraphQLRequest(query=SUBMIT_QUESTIONNAIRE_EXAMPLE, variables=variables)
)
