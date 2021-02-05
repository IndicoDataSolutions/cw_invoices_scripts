from solutions_toolkit.auto_review.config import AutoReviewConfiguration
from solutions_toolkit.auto_review.reviewer import Reviewer
from solutions_toolkit.auto_review.field_config import FIELD_CONFIG
from solutions_toolkit.indico_wrapper import IndicoWrapper


EXCEPTION_STATUS = "PENDING_ADMIN_REVIEW"
AUTO_REVIEW_STATUS = "PENDING_AUTO_REVIEW"
PENDING_REVIEW_STATUS = "PENDING_REVIEW"
COMPLETE_STATUS = "COMPLETE"


if __name__ == "__main__":

    # TODO: make this an input arg
    configuration_file = "/home/fitz/Documents/customers/cushman-wakefield/invoices/cw_invoices_scripts/solutions_toolkit/auto_review/config.yaml"
    config = AutoReviewConfiguration.from_yaml(configuration_file)

    indico_wrapper = IndicoWrapper(config.host, config.api_token_path)

    auto_review_submissions = indico_wrapper.get_submissions(
        config.workflow_id, AUTO_REVIEW_STATUS, retrieved_flag=False
    )

    for submission in auto_review_submissions:
        results = indico_wrapper.get_workflow_output(submission)

        inital_predictions = results["results"]["document"]["results"]
        reviewer = Reviewer(inital_predictions, config.model_name, FIELD_CONFIG)
        reviewer.apply_reviews()
        updated_predictions = reviewer.get_updated_predictions()

        # Note: this is a breaking call because we update the storage object
        # need to be careful with handling this
        job = indico_wrapper.submit_updated_review(submission, updated_predictions)
