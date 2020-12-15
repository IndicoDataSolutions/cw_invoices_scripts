import os
import pandas as pd
from collections import defaultdict
from indico.queries import (
    RetrieveStorageObject,
    SubmissionResult,
    UpdateSubmission,
    SubmissionFilter,
    ListSubmissions,
    SubmitReview,
    JobStatus
)
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

AUTO_REVIEW_STATUS = "PENDING_AUTO_REVIEW"
PENDING_REVIEW_STATUS = "PENDING_REVIEW"


def get_submissions(client, workflow_id, status, retrieved):
    sub_filter = SubmissionFilter(status=status, retrieved=retrieved)
    complete_submissions = client.call(
        ListSubmissions(workflow_ids=[workflow_id], filters=sub_filter)
    )
    return complete_submissions


def auto_review_supplier_name(prediction):
    confidence = prediction["confidence"]["Supplier Name"]
    if confidence > .98:
        prediction[e"accepted"] = True
    elif confidence < .5:
        prediction["rejcted"] = True
    return prediction


def auto_review_by_confidence(prediction, label, low_conf, high_conf):
    confidence = prediction["confidence"][label]
    if confidence > high_conf:
        prediction["accepted"] = True
    elif confidence < low_conf:
        prediction["rejected"] = True
    return prediction


if __name__ == "__main__":
    retrieved = False
    auto_review_submissions = get_submissions(INDICO_CLIENT, WORKFLOW_ID, AUTO_REVIEW_STATUS, retrieved)

    for submission in auto_review_submissions:
        results = INDICO_CLIENT.call(RetrieveStorageObject(submission.result_file))

        updated_predictions = results["results"]["document"]["results"]
        
        for prediction in updated_predictions[MODEL_NAME]:                
            if prediction["label"] == "Supplier Name":
                auto_review_supplier_name(prediction)
            
        job = INDICO_CLIENT.call(SubmitReview(submission.id, changes=updated_predictions))
    
    pending_review_submissions = get_submissions(INDICO_CLIENT, WORKFLOW_ID, PENDING_REVIEW_STATUS, retrieved)
    submission = pending_review_submissions[0]
    results = INDICO_CLIENT.call(RetrieveStorageObject(submission.result_file))
    assert "accepted" in results["results"]["document"]["results"][MODEL_NAME]
