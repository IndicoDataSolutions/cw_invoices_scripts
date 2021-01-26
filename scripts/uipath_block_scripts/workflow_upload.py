"""
This script is meant to handle the uploading of invoices to the Indico model
workflow.  Note that this script is where we would want to place logic for
STP.
"""

from indico.queries import WorkflowSubmission

from utils import files_from_directory, move_file
from config import WORKFLOW_ID, INDICO_CLIENT


# TODO: Change this to the appropriate shared drive
# Should this belong to config?  Or do we want to make these user inputs?
INVOICE_INPUT_DIR = "/home/fitz/Documents/customers/cushman-wakefield/GOS/model_retrain_with_csv/data/invoice_docs"
UPLOADED_DIR = "/home/fitz/Documents/customers/cushman-wakefield/invoices/uploaded"


def upload_to_workflow(pdf_filepaths, client, workflow_id, batch_size=10):
    submission_ids = []
    for batch_start in range(0, len(pdf_filepaths), batch_size):
        batch_end = batch_start + batch_size
        pdf_batch = pdf_filepaths[batch_start:batch_end]

        batch_submission_ids = client.call(
            WorkflowSubmission(workflow_id=workflow_id, files=pdf_batch)
        )
        submission_ids.append(batch_submission_ids)
    return submission_ids


if __name__ == "__main__":
    pdf_filepaths = files_from_directory(INVOICE_INPUT_DIR, "*.pdf")
    submision_ids = upload_to_workflow(pdf_filepaths, INDICO_CLIENT, WORKFLOW_ID)

    # move pdfs to completed folder
    for pdf_filepath in pdf_filepaths:
        move_file(pdf_filepath, UPLOADED_DIR)

