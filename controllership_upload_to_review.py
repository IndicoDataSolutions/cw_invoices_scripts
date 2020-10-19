"""
This script is meant to handle the uploading of invoices to the Indico model
workflow.  Note that this script is where we would want to place logic for
STP.
"""
import os
import glob
import shutil


def files_from_directory(src_dir, regex="*.*"):
    """
    return a list of all files in src_dir that match the regex
    """
    filename_regex = os.path.join(src_dir, regex)
    filelist = glob.glob(filename_regex)
    return filelist


def move_file(filepath, outdir):
    shutil.move(filepath, outdir)

from indico.queries import WorkflowSubmission

import os
from indico import IndicoClient, IndicoConfig


HOST = os.getenv("INDICO_API_HOST", "cush.indico.domains")
API_TOKEN_PATH = "C:\\Users\\Gnana Peddi\\Downloads\\indico_api_token (10).txt"
with open(API_TOKEN_PATH) as f:
    API_TOKEN = f.read()

my_config = IndicoConfig(host=HOST, api_token=API_TOKEN, verify_ssl=False)
INDICO_CLIENT = IndicoClient(config=my_config)

# NOTE, please configure this to the appropriate WORKFLOW_ID
WORKFLOW_ID = 3

# TODO: Change this to the appropriate shared drive
INVOICE_INPUT_DIR = "C:\\Users\\Gnana Peddi\\Downloads\\Test Folder\\"
UPLOADED_DIR = "C:\\Users\\Gnana Peddi\\Downloads\\Test Completed Folder\\"


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

    # move pdfs to completed folder after being uploaded to review
    for pdf_filepath in pdf_filepaths:
        move_file(pdf_filepath, UPLOADED_DIR)
