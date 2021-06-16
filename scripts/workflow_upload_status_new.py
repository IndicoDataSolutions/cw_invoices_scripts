"""
This script is meant to handle the uploading of invoices to the Indico model
workflow.  Note that this script is where we would want to place logic for
STP.
"""

import sys
import os
from indico import config
from solutions_toolkit import indico_wrapper
from tqdm import tqdm
import pandas as pd
import datetime
from solutions_toolkit.uipath_block_scripts.utils import files_from_directory, move_file
from solutions_toolkit.uipath_block_scripts.config import ExportConfiguration
from solutions_toolkit.indico_wrapper import IndicoWrapper


USAGE_STRING = "USAGE: python3 workflow_upload.py path/to/config.yaml"

class WorkflowUpload:
    def __init__(self, config):
        self.batch_size = config.upload_batch_size
        self.workflow_id = config.workflow_id
        self.uploaded_dir = config.uploaded_dir
        self.submissions_df=pd.read_csv(config.submissions_csv)
        self.timeout = config.timeout
        self.indico_wrapper = IndicoWrapper(config.host, config.api_token_path)

    def upload_to_workflow(self,submision_ids, wait=True):
        total_uploaded = len(submision_ids)
        batch_count = 0
        subs = []
        for batch_start in tqdm(range(0, total_uploaded, self.batch_size)):
            batch_end = batch_start + self.batch_size
            batch = submision_ids[batch_start:batch_end]
            for sub in batch:
                subs.append(sub)

            if wait:
                self.indico_wrapper.wait_for_submission(batch, timeout=self.timeout)

            batch_count += len(batch)
            print(f"\ncompleted upload of {batch_count}/{total_uploaded}")
        return subs


    def submission_status_csv(self,subs):
        submissions = []
        for sub in subs:
            submissions.append(sub)
        submission_obj = []
        for sub in submissions:
            submission_obj.append(self.indico_wrapper.get_submission(sub))
        sub_id = []
        sub_filename = []
        sub_status=[]
        sub_error=[]
        for sub in submission_obj:
            sub_id.append(sub.id)
            sub_filename.append(sub.input_filename)
            sub_status.append(sub.status)
            sub_error.append(sub.errors)

        submissions_df = pd.DataFrame(
            {
                "Submission ID": sub_id,
                "File Name": sub_filename,
                "Submission Status": sub_status,
                "Submission Error": sub_error
            }
        )
        timestamp = datetime.datetime.now().strftime("%m_%d_%Y-%I_%M_%S_%p")
        submissions_path = os.path.join(self.uploaded_dir, f"Submission_Status_{timestamp}.csv")
        submissions_df.to_csv(submissions_path, index=False)

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
    submision_ids = workflow_upload.submissions_df['Submission ID'].tolist()
    subs = workflow_upload.upload_to_workflow(submision_ids)
    workflow_upload.submission_status_csv(subs)
    print("A file containing the current submissions and their status has been generated")
