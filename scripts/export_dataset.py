
import os
import pandas as pd
from indico import IndicoConfig, IndicoClient
from indico.queries import WorkflowSubmission, CreateExport, DownloadExport
import json

HOST = "cush.indico.domains"
API_TOKEN_PATH = "../../indico_api_token.txt"

my_config = IndicoConfig(host=HOST, api_token_path=API_TOKEN_PATH)
client = IndicoClient(config=my_config)

dataset_id = 49
export = client.call(CreateExport(dataset_id=dataset_id, file_info=True, wait=True))
csv = client.call(DownloadExport(export.id))

csv.to_csv("bank_rec_v2.csv", index=False)
 
