import yaml
from indico.queries import (
    RetrieveStorageObject,
    SubmissionFilter,
    ListSubmissions,
    SubmitReview,
    GetSubmission
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

    def get_submission(self, workflow_id, submission_id):
        return self.indico_client.call(GetSubmission(submission_id))

    def get_submissions(self, workflow_id, submission_status=None, retrieved_flag=None):
        sub_filter = SubmissionFilter(
            status=submission_status, retrieved=retrieved_flag
        )
        complete_submissions = self.indico_client.call(
            ListSubmissions(workflow_ids=[workflow_id], filters=sub_filter)
        )
        return complete_submissions

    def get_workflow_output(self, submission):
        return self.indico_client.call(RetrieveStorageObject(submission.result_file))

    def submit_updated_review(self, submission, updated_predictions):
        return self.indico_client.call(
            SubmitReview(submission.id, changes=updated_predictions)
        )


EXCEPTION_STATUS = "PENDING_ADMIN_REVIEW"
AUTO_REVIEW_STATUS = "PENDING_AUTO_REVIEW"
PENDING_REVIEW_STATUS = "PENDING_REVIEW"
COMPLETE_STATUS = "COMPLETE"


if __name__ == "__main__":

    # TODO: make this an input arg
    configuration_file = "/home/fitz/Documents/customers/cushman-wakefield/invoices/cw_invoices_scripts/scripts/auto_review/config.yaml"
    config = Configuration.from_yaml(configuration_file)

    indico_wrapper = IndicoWrapper(config.host, config.api_token_path)

    auto_review_submissions = indico_wrapper.get_submissions(
        config.workflow_id, AUTO_REVIEW_STATUS, retrieved=False
    )

    for submission in auto_review_submissions:
        results = indico_wrapper.get_workflow_output(submission)

        updated_predictions = results["results"]["document"]["results"]

        for prediction in updated_predictions:
            if prediction["label"] == "Supplier Name":
                auto_review_supplier_name(prediction)

        # Note: this is a breaking call because we update the storage object
        # need to be careful with handling this
        # job = indico_wrapper.submit_updated_review(submission, updated_predictions)

    """
    Sample test code when backend changes are made
    pending_review_submissions = indico_wrapper.get_submissions(config.workflow_id, PENDING_REVIEW_STATUS)
    
    submission = pending_review_submissions[0]
    results = indico_wrapper.get_workflow_output(submission)
    accepted_count = 0
    rejected_count = 0
    for prediction in results["results"]["document"]["results"][config.model_name]:
        if "accepted" in prediction:
            accepted_count += 1
        if "rejected" in prediction:
            rejected_count += 1
    print(f"# accepted: {accepted_count}, # rejected: {rejected_count}")
    """