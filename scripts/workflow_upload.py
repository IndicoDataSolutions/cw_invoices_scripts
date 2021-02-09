"""
This script is meant to handle the uploading of invoices to the Indico model
workflow.  Note that this script is where we would want to place logic for
STP.
"""

import sys
import os

from solutions_toolkit.uipath_block_scripts.utils import files_from_directory, move_file
from solutions_toolkit.uipath_block_scripts.config import ExportConfiguration
from solutions_toolkit.indico_wrapper import IndicoWrapper


USAGE_STRING = "USAGE: python3 workflow_upload.py path/to/config.yaml"


class WorkflowUpload:
    def __init__(self, config):
        self.batch_size = config.upload_batch_size
        self.workflow_id = config.workflow_id
        self.uploaded_dir = config.uploaded_dir
        self.timeout = config.timeout
        self.indico_wrapper = IndicoWrapper(config.host, config.api_token_path)

    def upload_to_workflow(self, pdf_filepaths, wait=False):
        submission_ids = []
        total_uploaded = len(pdf_filepaths)
        batch_count = 0
        for batch_start in range(0, len(pdf_filepaths), self.batch_size):
            batch_end = batch_start + self.batch_size
            pdf_batch = pdf_filepaths[batch_start:batch_end]

            batch_submission_ids = self.indico_wrapper.upload_to_workflow(
                self.workflow_id, pdf_batch
            )
            submission_ids.append(batch_submission_ids)

            if wait:
                self.indico_wrapper.wait_for_submission(batch_submission_ids, timeout=self.timeout)

            for pdf_filepath in pdf_batch:
                move_file(pdf_filepath, self.uploaded_dir)

            batch_count += len(pdf_batch)
            print(f"completed upload of {batch_count}/{total_uploaded}")
        return submission_ids


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(USAGE_STRING)
        sys.exit()

    configuration_path = sys.argv[1]
    if not os.path.exists(configuration_path):
        print(f"configuration file: {configuration_path} does not exist")
        sys.exit()

    config = ExportConfiguration.from_yaml(configuration_path)
    workflow_upload = WorkflowUpload(config)
    pdf_filepaths = files_from_directory(config.document_input_dir, "*.pdf")
    submision_ids = workflow_upload.upload_to_workflow(pdf_filepaths, wait=config.wait)
