"""
This script is meant to handle the uploading of invoices to the Indico model
workflow.  Note that this script is where we would want to place logic for
STP.
"""

import os
import glob
from indico.queries import WorkflowSubmission

from config import WORKFLOW_ID, INDICO_CLIENT


# TODO: Change this to the appropriate shared drive
# Should this belong to config?
INVOICE_INPUT_DIR = "/home/fitz/Documents/customers/cushman-wakefield/invoices/docs"
UPLOADED_DIR = "/home/fitz/Documents/customers/cushman-wakefield/invoices/uploaded"


def upload_to_workflow(pdf_filepaths, batch_size=10):
    submission_ids = []
    for batch_start in range(0, len(pdf_filepaths), batch_size):
        batch_end = batch_start + batch_size
        pdf_batch = pdf_filepaths[batch_start:batch_end]
        batch_submission_ids = INDICO_CLIENT.call(
            WorkflowSubmission(workflow_id=WORKFLOW_ID, files=pdf_batch)
        )
        submission_ids.append(batch_submission_ids)
    return submission_ids
