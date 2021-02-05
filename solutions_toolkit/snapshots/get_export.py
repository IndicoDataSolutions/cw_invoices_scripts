import os

from indico import IndicoConfig, IndicoClient
from indico.queries import (
    CreateExport,
    DownloadExport
)


output_dir = "/home/fitz/Documents/customers/cushman-wakefield/yardi-bank-rec/data/stack_review_data_02-02-021"
output_filename = "dataset_export.csv"
output_path = os.path.join(output_dir, output_filename)

DATASET_ID = 100
# NOTE, Configure
HOST = os.getenv("INDICO_API_HOST", "cush.indico.domains")

# NOTE, Configure
API_TOKEN_PATH = "/home/fitz/Documents/customers/cushman-wakefield/indico_api_token.txt"
with open(API_TOKEN_PATH) as f:
    API_TOKEN = f.read()

my_config = IndicoConfig(host=HOST, api_token=API_TOKEN, verify_ssl=False)
INDICO_CLIENT = IndicoClient(config=my_config)

export = INDICO_CLIENT.call(CreateExport(dataset_id=DATASET_ID, wait=True, file_info=True))
csv = INDICO_CLIENT.call(DownloadExport(export.id))

csv.to_csv(output_path, index=False)
