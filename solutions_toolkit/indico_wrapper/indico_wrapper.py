from indico.queries import (
    RetrieveStorageObject,
    SubmissionFilter,
    ListSubmissions,
    SubmitReview,
    GetSubmission,
    WorkflowSubmission,
    WaitForSubmissions,
)
from indico import IndicoClient, IndicoConfig


class IndicoWrapper:
    """
    Class to handle all indico api calls
    """

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

    def upload_to_workflow(self, workflow_id, pdf_filepaths):
        """
        Return a list of submission ids
        """
        return self.indico_client.call(
            WorkflowSubmission(workflow_id=workflow_id, files=pdf_filepaths)
        )

    def wait_for_submission(self, submission_ids, timeout=60):
        return self.indico_client.call(WaitForSubmissions(submission_ids=submission_ids, timeout=timeout))
 