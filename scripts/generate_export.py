"""
This script handles pulling COMPLETE documents from a workflow and transforming
the results into a csv format for further downstream processing.
"""
import os
import pandas as pd
from collections import defaultdict
from indico.queries import (
    RetrieveStorageObject,
    SubmissionResult,
    UpdateSubmission,
    GraphQLRequest,
    GetSubmission,
)

from config import KEY_FIELDS, ROW_FIELDS, INDICO_CLIENT, WORKFLOW_ID, MODEL_NAME


EXPORT_DIR = "/home/fitz/Documents/customers/cushman-wakefield/invoices/uploaded"


def assign_confidences(results, model_name):
    """
    Append confidences to predictions
    """
    preds_pre_review = results["results"]["document"]["results"][model_name]["pre_review"]
    preds_final = results["results"]["document"]["results"][model_name]["final"]

    for pred_final in preds_final:
        final_start = pred_final['start']
        final_end = pred_final['end']
        for pred_pre_review in preds_pre_review:
            pre_start = pred_pre_review['start']
            pre_end = pred_pre_review['end']
            if final_start == pre_start and final_end == pre_end:
                pred_final['confidence'] = pred_pre_review['confidence']
    return preds_final


def get_page_extractions(submission, model_name, post_review=True):
    """
    Return predictions and page info for a submission

    post_review is a flag to select either the final reviewed values
    or the pre reviewed values
    """
    if post_review:
        result_type = "final"
    else:
        result_type = "pre_review"

    sub_job = INDICO_CLIENT.call(SubmissionResult(submission.id, wait=True))
    results = INDICO_CLIENT.call(RetrieveStorageObject(sub_job.result))

    # get page info
    etl_output_url = results["etl_output"]
    etl_output = INDICO_CLIENT.call(RetrieveStorageObject(etl_output_url))

    page_infos = []
    for page in etl_output["pages"]:
        page_info_url = page["page_info"]
        page_text_dict = INDICO_CLIENT.call(RetrieveStorageObject(page_info_url))
        page_infos.append(page_text_dict)

    # get predictions
    if result_type == "pre_review":
        predictions = results["results"]["document"]["results"][model_name][result_type]
    else:
        predictions = assign_confidences(results, model_name)
    return page_infos, predictions


def merge_page_tokens(pages):
    """
    Return a list of tokens from a list of pages
    """
    tokens = []
    for page in pages:
        tokens.extend(page["tokens"])
    return tokens


def filter_preds(predictions, label_set):
    return [pred for pred in predictions if pred["label"] in label_set]


def align_rows(row_predictions, tokens, filename):
    """
    Main logic for aligning data
    row_predictions: a list of line item predictions from 1 document
    tokens: list of token dictionaries from pdf extraction
    filename: string name of file predicted on
    """

    # Indico prediction positions are given as spans in the document string
    # these spans need to be resolved with the positioning of the tokens in the doc
    positions = list()
    page_preds = sorted(row_predictions, key=lambda x: x["start"])
    for pred in page_preds:
        start, end = (
            pred["start"] - 1,
            pred["end"] + 1,
        )
        position = dict()
        for token in tokens:
            if (
                token["doc_offset"]["start"] >= start
                and token["doc_offset"]["end"] <= end
            ):
                position["bbtop"] = token["position"]["bbTop"]
                position["bbbot"] = token["position"]["bbBot"]
                position["label"] = pred["label"]
                position["text"] = pred["text"]
                # handle the case where labels are adjusted in review and contain no confidence
                if "confidence" in pred:
                    position["confidence"] = pred["confidence"][position["label"]]
                else:
                    position["confidence"] = 1.0
                break
        if not position:
            continue
        positions.append(position)

    # we now group these values by their y position in the page
    new_list = []
    new_positions = sorted(positions, key=lambda x: x["bbtop"])
    max_top = new_positions[0]["bbtop"]
    min_bot = new_positions[0]["bbbot"]
    row = defaultdict(list)
    for i in new_positions:
        if i["bbtop"] > min_bot:
            new_list.append(row)
            row = defaultdict(list)
            max_top, min_bot = i["bbtop"], i["bbbot"]
            row[i["label"]].append(i)
        else:
            row[i["label"]].append(i)
            max_top, min_bot = max(i["bbtop"], max_top), min(i["bbbot"], min_bot)
    new_list.append(row)

    # convert to a data frame
    line_item_df = aligned_rows_to_df(new_list)
    line_item_df["filename"] = filename
    return line_item_df


def predictions_to_df(submissions, doc_predictions):
    """
    Convert list of prediction dicts to a vertical df
    with columns [filename, label, value, confidence]
    """
    pred_dict = defaultdict(list)
    for sub, doc_prediction in zip(submissions, doc_predictions):
        for prediction in doc_prediction:
            label = prediction["label"]
            pred_dict["label"].append(label)
            pred_dict["text"].append(prediction["text"])
            if "confidence" in prediction:
                pred_dict["confidence"].append(prediction["confidence"][label])
            else:
                pred_dict["confidence"].append(1.0)
            pred_dict["filename"] = sub.input_filename

    pred_df = pd.DataFrame(pred_dict)
    return pred_df


def get_top_pred_df(pred_df):
    pred_highest_df = pred_df.loc[
        pred_df.groupby(["filename", "label"])["confidence"].idxmax()
    ]
    return pred_highest_df


def vert_to_horizontal(top_conf_df):
    """
    Assumes that df only contains one value per label
    """
    pivot_val_cols = ["text", "confidence"]
    dfs = []
    for pivot_val_col in pivot_val_cols:
        columns = ["filename", "label", pivot_val_col]
        subset_df = top_conf_df[columns].set_index(["filename", "label"])
        pred_comparison_df = subset_df.unstack()[pivot_val_col].add_suffix(
            f" {pivot_val_col}"
        )
        dfs.append(pred_comparison_df)

    pred_wide_df = pd.concat(dfs, axis=1)

    # reorder columns
    labels = pd.unique(top_conf_df["label"]).tolist()
    col_order = []
    for label in labels:
        for pivot_val_col in pivot_val_cols:
            col_order.append(f"{label} {pivot_val_col}")
    return pred_wide_df[col_order].reset_index()


def aligned_rows_to_df(aligned_rows):
    df_rows = []
    for row in aligned_rows:
        df_row = defaultdict(str)
        for label, values in row.items():
            df_row[f"{label} confidence"] = []
            # concatenate multiple values
            # TODO: need to investigate whether this is a correct approach
            for value in values:
                df_row[f"{label} text"] += str(value["text"])
                df_row[f"{label} confidence"].append(value["confidence"])
            if len(df_row[f"{label} confidence"]) == 1:
                df_row[f"{label} confidence"] = df_row[f"{label} confidence"][0]
        df_rows.append(df_row)

    return pd.DataFrame(df_rows)


def get_submissions(client, workflow_id, status, retrieved):
    qstr = f"""{{
        submissions( filters: {{status: {status}}}){{
            submissions{{
            id
            inputFile
            resultFile
            status
            retrieved
            workflowId
            }}
        }}
    }}"""
    subs = client.call(GraphQLRequest(query=qstr))["submissions"]["submissions"]
    subs = [s for s in subs if s["workflowId"] == workflow_id and s["status"] == status]
    # TODO: make this make more sense
    if not retrieved:
        subs = [s for s in subs if not s["retrieved"]]

    subs = [client.call(GetSubmission(sub["id"])) for sub in subs]
    return subs


def mark_retreived(client, submission_id):
    client.call(UpdateSubmission(submission_id, retrieved=True))


if __name__ == "__main__":
    retrieved = False
    status = "COMPLETE"
    complete_submissions = get_submissions(
        INDICO_CLIENT, WORKFLOW_ID, status, retrieved
    )

    # FULL WORK FLOW
    full_dfs = []
    for submission in complete_submissions:
        page_infos, predictions = get_page_extractions(
            submission, MODEL_NAME, post_review=True
        )
        tokens = merge_page_tokens(page_infos)

        key_predictions = filter_preds(predictions, KEY_FIELDS)
        row_predictions = filter_preds(predictions, ROW_FIELDS)

        line_item_df = align_rows(row_predictions, tokens, submission.input_filename)
        key_preds_vert_df = predictions_to_df([submission], [key_predictions])
        top_conf_key_pred_df = get_top_pred_df(key_preds_vert_df)
        key_pred_df = vert_to_horizontal(top_conf_key_pred_df)
        full_df = key_pred_df.merge(line_item_df, on=["filename"])
        full_dfs.append(full_df)

    output_df = pd.concat(full_dfs)

    labels = KEY_FIELDS + ROW_FIELDS
    col_order = ['filename']
    pivot_val_cols = ["text", "confidence"]
    for label in labels:
        for pivot_val_col in pivot_val_cols:
            col_order.append(f"{label} {pivot_val_col}")

    current_cols = set(output_df.columns)
    missing_cols = list(set(col_order).difference(current_cols))
    output_df[missing_cols] = None

    output_df = output_df[col_order]

    output_filepath = os.path.join(EXPORT_DIR, "export.csv")
    output_df.to_csv(output_filepath, index=False)

    for sub in complete_submissions:
        mark_retreived(INDICO_CLIENT, sub["id"])

    output_df.head()
