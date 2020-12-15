import os
from indico import IndicoClient, IndicoConfig


HOST = os.getenv("INDICO_API_HOST", "cush.indico.domains")
API_TOKEN = os.getenv("CW_INDICO_API_TOKEN")

my_config = IndicoConfig(host=HOST, api_token=API_TOKEN)
INDICO_CLIENT = IndicoClient(config=my_config)

# NOTE, please configure this to the appropriate ID
WORKFLOW_ID = 13

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
