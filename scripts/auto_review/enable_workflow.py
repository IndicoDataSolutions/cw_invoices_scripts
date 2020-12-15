import os

from indico.queries import UpdateWorkflowSettings
from indico import IndicoClient, IndicoConfig


# NOTE, Configure
HOST = os.getenv("INDICO_API_HOST", "cush.indico.domains")

# NOTE, Configure
API_TOKEN_PATH = "../../indico_api_token.txt"
with open(API_TOKEN_PATH) as f:
    API_TOKEN = f.read()

my_config = IndicoConfig(host=HOST, api_token=API_TOKEN, verify_ssl=False)
INDICO_CLIENT = IndicoClient(config=my_config)
EXCEPTION_STATUS = "PENDING_ADMIN_REVIEW"
COMPLETE_STATUS = "COMPLETE"

# NOTE, please configure this to the appropriate ID
WORKFLOW_ID = 141

# NOTE, please configure this to the appropriate model name
MODEL_NAME = "GOS Invoice test model"

workflow = INDICO_CLIENT.call(
    UpdateWorkflowSettings(WORKFLOW_ID, enable_review=True, enable_auto_review=True)
)
