import os
from indico import IndicoClient, IndicoConfig


HOST = "cush.indico.domains"

# NOTE, Configure
API_TOKEN_PATH = "../../indico_api_token.txt"

# boiler plate code in every script using indico calls
with open(API_TOKEN_PATH) as f:
    API_TOKEN = f.read()

my_config = IndicoConfig(host=HOST, api_token=API_TOKEN, verify_ssl=False)
INDICO_CLIENT = IndicoClient(config=my_config)

# NOTE, please configure this to the appropriate ID
WORKFLOW_ID = 150

# NOTE, please configure this to the appropriate model name
MODEL_NAME = 'Invoice Extraction Model 2.0 q8 model'

# Field types
# There should only be one value for each key field
KEY_FIELDS = [
    "PO#",
    "Company Name",
    "Invoice Number",
    "Due Date",
    "Supplier Name",
    "Remit to Address (Street)",
    "Remit to Address (City)",
    "Remit to Address (State)",
    "Remit to Address (Zip Code)",
    "Invoice Amount",
    "Currency",
    "Tax Amount",
    "Freight Amount",
    "Line Item Number",
    "Currency",
]

# Row fields will be aggregated into rows
ROW_FIELDS = [
    "Unit Cost",
    "Quantity",
    "Extended Amount",
    "Line Item Description",
    "Week End / Activity",
    "Other Charges",
]
