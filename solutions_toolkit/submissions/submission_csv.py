"""
Create a csv from COMPLETE review queue submissions to add to an existing
prelabeld csv
"""
import json
import pandas as pd
from random import shuffle
from solutions_toolkit.indico_wrapper import IndicoWrapper


def validate_predictions(results, predictions):
    for pred in predictions:
        if validate_pred_as_label(pred):
            continue
        else:
            return False
    return True


def validate_pred_as_label(pred):
    # ensure predictions contain valid spans
    start = pred["start"]
    end = pred["end"]
    if start is None or end is None:
        return False
    else:
        return True


def convert_predictions_to_labels(predictions):
    labels = []
    keys_to_remove = ["text", "confidence"]
    for pred in predictions:
        for key in keys_to_remove:
            pred.pop(key, None)
        labels.append(pred)
    return labels


def get_page_text(indico_wrapper, etl_output_url):
    etl_output = indico_wrapper.get_storage_object(etl_output_url)
    page_texts = []
    for page in etl_output["pages"]:
        page_info_url = page["page_info"]
        page_text_dict = indico_wrapper.get_storage_object(page_info_url)
        page_text = page_text_dict["pages"][0]["text"]

        page_texts.append(page_text)
    return "\n".join(page_texts)


# config stuff
HOST = "prod-cush.indico.domains"
API_TOKEN_PATH = (
    "/home/fitz/Documents/customers/cushman-wakefield/prod_indico_api_token.txt"
)
WORKFLOW_ID = 12
N_ADDITIONAL_SAMPLES = 50
MODEL_NAME = "Procurement COI q6 model"

indico_wrapper = IndicoWrapper(HOST, API_TOKEN_PATH)
complete_submissions = indico_wrapper.get_submissions(
    WORKFLOW_ID, submission_status="COMPLETE", retrieved_flag=True
)
shuffle(complete_submissions)

csv_rows = []
for submission in complete_submissions:
    if len(csv_rows) == N_ADDITIONAL_SAMPLES:
        break

    results = indico_wrapper.get_submission_results(submission)

    if results.get("review_rejected"):
        continue

    page_text = get_page_text(indico_wrapper, results["etl_output"])
    filename = submission.input_filename
    pred_labels = results["results"]["document"]["results"][MODEL_NAME]["final"]

    if not validate_predictions(results, pred_labels):
        continue

    labels = convert_predictions_to_labels(pred_labels)
    label_string = json.dumps(labels)
    csv_row = {"text": page_text, "filename": filename, "labels": label_string}
    csv_rows.append(csv_row)

label_df = pd.DataFrame(csv_rows)
label_df.to_csv("coi_retraining_data.csv", index=False)
