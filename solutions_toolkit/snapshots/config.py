import os
from indico import IndicoClient, IndicoConfig

HOST = os.getenv("INDICO_API_HOST", "cush.indico.domains")

API_TOKEN_PATH = "/home/fitz/Documents/customers/cushman-wakefield/indico_api_token.txt"
with open(API_TOKEN_PATH) as f:
    API_TOKEN = f.read()

my_config = IndicoConfig(host=HOST, api_token=API_TOKEN, verify_ssl=False)

INDICO_CLIENT = IndicoClient(config=my_config)

COMPLETE_STATUS = "COMPLETE"

DATASET_ID = 116

SNAPSHOT_DIR = "/home/fitz/Documents/customers/cushman-wakefield/COI/data/merge_02-03-21"

UNLABELED_FIELDS = []
DUMMY_FIELDS = []