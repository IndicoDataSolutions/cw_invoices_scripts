import os
import yaml
import pandas as pd
from collections import defaultdict
from indico.queries import (
    RetrieveStorageObject,
    SubmissionResult,
    UpdateSubmission,
    SubmissionFilter,
    ListSubmissions,
    SubmitReview,
    JobStatus,
)
from indico import IndicoClient, IndicoConfig


# TODO: need to do config checks for error catching
class Configuration:
    def __init__(self, config):
        self.host = config.get("host")
        self.api_token_path = config.get("api_token_path")
        self.workflow_id = config.get("workflow_id")
        self.model_name = config.get("model_name")

    @classmethod
    def from_yaml(cls, yaml_filepath):
        with open(yaml_filepath) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        return cls(config)


# TODO: continue adding functionality
class IndicoWrapper:
    def __init__(self, host, api_token_path):
        self.host = host
        self.api_token_path = api_token_path
        with open(api_token_path) as f:
            self.api_token = f.read().strip()
        my_config = IndicoConfig(
            host=self.host, api_token=self.api_token, verify_ssl=False
        )
        self.indico_client = IndicoClient(config=my_config)

    def get_submissions(self, workflow_id, status, retrieved):
        sub_filter = SubmissionFilter(status=status, retrieved=retrieved)
        complete_submissions = self.indico_client.call(
            ListSubmissions(workflow_ids=[workflow_id], filters=sub_filter)
        )
        return complete_submissions


EXCEPTION_STATUS = "PENDING_ADMIN_REVIEW"
AUTO_REVIEW_STATUS = "PENDING_AUTO_REVIEW"
PENDING_REVIEW_STATUS = "PENDING_REVIEW"
COMPLETE_STATUS = "COMPLETE"


def auto_review_supplier_name(prediction):
    confidence = prediction["confidence"]["Supplier Name"]
    if confidence > 0.98:
        prediction["accepted"] = True
    elif confidence < 0.5:
        prediction["rejected"] = True
    return prediction


def auto_review_by_confidence(prediction, label, low_conf, high_conf):
    confidence = prediction["confidence"][label]
    if confidence > high_conf:
        prediction["accepted"] = True
    elif confidence < low_conf:
        prediction["rejected"] = True
    return prediction


"""
Idea for how to apply functions to a class 
{"Supplier Name": [(fn, high_conf, low_conf), (fn, text_length, threshold)]}
"""
if __name__ == "__main__":

    configuration_file = "./config.yaml"
    config = Configuration.from_yaml(configuration_file)

    indico_wrapper = IndicoWrapper(config.host, config.api_token_path)
    indico_client = indico_wrapper.indico_client

    retrieved = False

    auto_review_submissions = indico_wrapper.get_submissions(
        config.workflow_id, AUTO_REVIEW_STATUS, retrieved
    )

    for submission in auto_review_submissions:
        # TODO: add this call to IndicoWrapper
        results = indico_client.call(RetrieveStorageObject(submission.result_file))

        updated_predictions = results["results"]["document"]["results"]

        for prediction in updated_predictions:
            if prediction["label"] == "Supplier Name":
                auto_review_supplier_name(prediction)

        # TODO: add this call to IndicoWrapper
        job = indico_client.call(
            SubmitReview(submission.id, changes=updated_predictions)
        )

    # pending_review_submissions = get_submissions(INDICO_CLIENT, WORKFLOW_ID, PENDING_REVIEW_STATUS, retrieved)
    # submission = pending_review_submissions[0]
    # results = INDICO_CLIENT.call(RetrieveStorageObject(submission.result_file))
    # assert "accepted" in results["results"]["document"]["results"][MODEL_NAME]
