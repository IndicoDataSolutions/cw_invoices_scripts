from indico.queries import (
    RetrieveStorageObject,
    SubmissionResult,
    ListSubmissions,
    SubmissionFilter,
)

import os
import glob
import pandas as pd

def get_submissions(client, workflow_id, status, retrieved):
    sub_filter = SubmissionFilter(status=status, retrieved=retrieved)
    complete_submissions = client.call(
        ListSubmissions(workflow_ids=[workflow_id], filters=sub_filter)
    )
    return complete_submissions

def get_submission_labels(client, submission, model_name):
    filepath = submission.input_filename
    result_url = client.call(SubmissionResult(submission.id, wait=True))
    results = client.call(RetrieveStorageObject(result_url.result))
    
    if results.get("review_rejected"):
        return None
    
    labels = results["results"]["document"]["results"][model_name]["final"]
    return labels

def sequences_overlap(true_seq, pred_seq):
    """
    Boolean return value indicates whether or not seqs overlap
    """
    start_contained = (
        pred_seq["start"] < true_seq["end"] and pred_seq["start"] >= true_seq["start"]
    )
    end_contained = (
        pred_seq["end"] > true_seq["start"] and pred_seq["end"] <= true_seq["end"]
    )
    return start_contained or end_contained


def find_overlaps(preds1, preds2):
    overlaps = []
    for pred1 in preds1:
        for pred2 in preds2:
            # note if you are comparing predictions in the same labelset this
            # will trigger when comparing pred1 to pred1
            if sequences_overlap(pred2, pred1) and pred2["label"] != pred1["label"]:
                overlaps.append(
                    (
                        pred1["label"],
                        pred2["label"],
                        pred1["start"] - pred2["start"],
                        pred1["end"] - pred2["end"],
                    )
                )
    return overlaps

def read_export(export_path, dataset_id):
    export_df = pd.read_csv(export_path)
    # assumes the dataset id will always be at the end of the column name
    export_df.rename(lambda x: x.replace(f"_{dataset_id}", ""), axis="columns", inplace=True)
    final_columns = [f"row_index", f"file_name", "text"]
    return export_df[final_columns]


def get_snapshot_files(snapshot_dir):
    file_regex = os.path.join(snapshot_dir, "*.csv")
    snapshot_filepaths = glob.glob(file_regex)
    return snapshot_filepaths
