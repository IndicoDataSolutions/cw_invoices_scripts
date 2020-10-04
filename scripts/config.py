import os
from indico import IndicoClient, IndicoConfig


HOST = os.getenv("INDICO_API_HOST", "cush.indico.domains")
API_TOKEN = os.getenv("INDICO_API_TOKEN")

my_config = IndicoConfig(host=HOST, api_token=API_TOKEN)
INDICO_CLIENT = IndicoClient(config=my_config)

# NOTE, please configure this to the appropriate ID
WORKFLOW_ID = 13
