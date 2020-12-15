"""
CreateCrowdLabel ~ Creates teach task

GetCrowdLabelQuestionaire ~ Contains labelset id

~submit labels~
for row_index, label in snapshot:
    SubmitQuestionarieExample
"""
import os
import pandas as pd

from indico import IndicoClient, IndicoConfig
from indico.queries import (
    GraphQLRequest,
    GetDataset
)

from graphql_queries import (
    CREATE_TEACH_TASK,
    GET_TEACH_TASK,
    SUBMIT_QUESTIONNAIRE_EXAMPLE
)


def create_teach_task(client, dataset_id, teach_task_name):
    dataset = client.call(GetDataset(dataset_id))
    source_col_id = dataset.datacolumn_by_name("text").id
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
                "targets": labels,
                "keywords": [],
                "text": teach_task_name,
            }
        ],
    }
    teach_task = client.call(GraphQLRequest(query=CREATE_TEACH_TASK, variables=variables))
    teach_task_id = teach_task["createQuestionnaire"]['id']
    variables = {"id": teach_task_id}
    teach_task_stats = INDICO_CLIENT.call(GraphQLRequest(query=GET_TEACH_TASK, variables=variables))
    labelset_id = teach_task_stats["questionnaires"]["questionnaires"][0]["questions"][0]['labelset']["id"]
    model_group_id = teach_task_stats["questionnaires"]["questionnaires"][0]["questions"][0]["modelGroupId"]
    return labelset_id, model_group_id


def label_teach_task(client, dataset_id, labelset_id, model_group_id, label_df, label_col):
    for _, row in label_df.iterrows():
        row_index = row.row_index
        labels = row[label_col]
        variables = {
            "datasetId": dataset_id,
            "labelsetId": labelset_id,
            "labels": [
                {"rowIndex": row_index, "target": labels, }
            ],
            "modelGroupId": model_group_id,
        }

        client.call(GraphQLRequest(query=SUBMIT_QUESTIONNAIRE_EXAMPLE, variables=variables))


# NOTE, Configure
HOST = os.getenv("INDICO_API_HOST", "cush.indico.domains")

# NOTE, Configure
API_TOKEN_PATH = "../../indico_api_token.txt"
with open(API_TOKEN_PATH) as f:
    API_TOKEN = f.read()

my_config = IndicoConfig(host=HOST, api_token=API_TOKEN, verify_ssl=False)
INDICO_CLIENT = IndicoClient(config=my_config)

DATASET_ID = 79

labels = [
    "Bill To Name",
    "Client Name",
    "Client PO#",
    "Currency",
    "Extended Amount",
    "Freight Amount",
    "Invoice Amount",
    "Invoice Date",
    "Invoice Line Description",
    "Invoice Number",
    "Remit to Address (City)",
    "Remit to Address (State)",
    "Remit to Address (Street)",
    "Remit to Address (Zip Code)",
    "Service Date",
    "Service Location Name",
    "Supplier Name",
    "Tax Amount",
]

labelset_id, model_group_id = create_teach_task(INDICO_CLIENT, DATASET_ID, "Test GOS Invoice Task")
variables = {"id": teach_task_id}
teach_task_stats = INDICO_CLIENT.call(GraphQLRequest(query=GET_TEACH_TASK, variables=variables))
labelset_id = teach_task_stats["questionnaires"]["questionnaires"][0]["questions"][0]['labelset']["id"]
model_group_id = teach_task_stats["questionnaires"]["questionnaires"][0]["questions"][0]["modelGroupId"]

label_csv_filename = "/home/fitz/Documents/customers/cushman-wakefield/va-invoices/add_reviewed_data_project/data/merged_teach_tasks.csv"
label_csv = pd.read_csv(label_csv_filename)

for _, row in label_csv.iterrows():
    row_index = row.row_index
    target = row.question
    variables = {
        "datasetId": DATASET_ID,
        "labelsetId": labelset_id,
        "labels": [
            {"rowIndex": row_index, "target": target,}
        ],
        "modelGroupId": model_group_id,
    }
    
    INDICO_CLIENT.call(GraphQLRequest(query=SUBMIT_QUESTIONNAIRE_EXAMPLE, variables=variables))
