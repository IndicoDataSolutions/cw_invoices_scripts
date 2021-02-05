import os
import glob
import pandas as pd

from indico.queries import GraphQLRequest, GetDataset

from solutions_toolkit.snapshots.config import (
    INDICO_CLIENT,
    COMPLETE_STATUS,
    DATASET_ID,
    DUMMY_FIELDS,
    UNLABELED_FIELDS,
    SNAPSHOT_DIR,
)
from solutions_toolkit.snapshots.utils import  get_snapshot_files
from solutions_toolkit.snapshots.snapshot import Snapshot
from solutions_toolkit.snapshots.queries import CREATE_TEACH_TASK, GET_TEACH_TASK, SUBMIT_QUESTIONNAIRE_EXAMPLE


snapshot_filepaths = get_snapshot_files(SNAPSHOT_DIR)
snapshots = [Snapshot.from_csv(s, text_col="text") for s in snapshot_filepaths]
merged_snapshot = Snapshot.merge_snapshots(snapshots)
merged_df = merged_snapshot.to_df()

model_fields = merged_snapshot.get_label_list()
full_field_list = model_fields + DUMMY_FIELDS + UNLABELED_FIELDS

# first create teach task
dataset = INDICO_CLIENT.call(GetDataset(DATASET_ID))
source_col_id = dataset.datacolumn_by_name("text").id
teach_task_name = "Procurement COI 02-03-21 V1"
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
