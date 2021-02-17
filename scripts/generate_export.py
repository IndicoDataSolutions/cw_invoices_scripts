import os
import sys
import time
from indico.client.client import IndicoClient
import pandas as pd
from collections import defaultdict
from solutions_toolkit.auto_review import Reviewer, FieldConfiguration
from solutions_toolkit.uipath_block_scripts.config import ExportConfiguration
from solutions_toolkit.indico_wrapper import IndicoWrapper
from indico.queries import (
    RetrieveStorageObject,
        SubmissionResult
)

USAGE_STRING = (
    "USAGE: python3 generate_export path/to/configuration_file"
)


def assign_confidences(results, model_name):
    """
    Append confidences to predictions
    """
    preds_pre_review = results["results"]["document"]["results"][model_name][
        "pre_review"
    ]
    preds_final = results["results"]["document"]["results"][model_name]["final"]

    for pred_final in preds_final:
        final_start = pred_final["start"]
        final_end = pred_final["end"]
        for pred_pre_review in preds_pre_review:
            pre_start = pred_pre_review["start"]
            pre_end = pred_pre_review["end"]
            if final_start == pre_start and final_end == pre_end:
                pred_final["confidence"] = pred_pre_review["confidence"]
    return preds_final


def get_page_extractions(indico_wrapper, submission, model_name, post_review=False):
    """
    Return predictions and page info for a submission
    post_review is a flag to select either the final reviewed values
    or the pre reviewed values
    """
    if post_review:
        result_type = "final"
    else:
        result_type = "pre_review"

    results = indico_wrapper.get_submission_results(submission)

    # get page info
    etl_output_url = results["etl_output"]
    etl_output = indico_wrapper.get_storage_object(etl_output_url)

    page_infos = []
    for page in etl_output["pages"]:
        page_info_url = page["page_info"]
        page_text_dict = indico_wrapper.get_storage_object(page_info_url)
        page_infos.append(page_text_dict)

    # get predictions
    if result_type == "pre_review":
        predictions = results["results"]["document"]["results"][model_name][result_type]
    else:
        # doc was rejected from review queue
        if results.get("review_rejected"):
            predictions = None
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


def add_page_number(page_predictions, tokens):
    for pred in page_predictions:
        if pred["start"]:
            start, end = (
                pred["start"] - 1,
                pred["end"] + 1,
            )
            for token in tokens:
                if (
                    token["doc_offset"]["start"] >= start
                    and token["doc_offset"]["end"] <= end
                ):
                    pred["page_num"] = token["page_num"]
                    break
            else:
                pred["page_num"] = 999


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
        position = dict()
        for token in tokens:
            if sequences_overlap(token["doc_offset"], pred):
                position["bbtop"] = token["position"]["bbTop"]
                position["bbbot"] = token["position"]["bbBot"]
                position["bbleft"] = token["position"]["bbLeft"]
                position["bbright"] = token["position"]["bbRight"]
                position["page_num"] = pred["page_num"]
                position["label"] = pred["label"]
                position["text"] = pred["text"]
                # handle the case where labels are adjusted in review and contain no confidence

                if pred.get("confidence"):
                    confidence = pred["confidence"].get(position["label"])
                    if confidence:
                        position["confidence"] = confidence
                    else:
                        position["confidence"] = 1.0
                else:
                    position["confidence"] = 1.0
                break
        if not position:
            continue
        positions.append(position)

    new_list = []
    if positions:
        # we now group these values by their y position in the page

        new_positions = sorted(
            positions, key=lambda x: (x["page_num"], x["bbtop"], x["bbleft"])
        )
        max_top = new_positions[0]["bbtop"]
        min_bot = new_positions[0]["bbbot"]
        current_page = new_positions[0]["page_num"]
        row = defaultdict(list)
        for i in new_positions:
            if i["bbtop"] > min_bot or i["page_num"] > current_page:
                new_list.append(row)
                row = defaultdict(list)
                max_top, min_bot, current_page = i["bbtop"], i["bbbot"], i["page_num"]
                row[i["label"]].append(i)
            else:
                row[i["label"]].append(i)
                max_top, min_bot = max(i["bbtop"], max_top), min(i["bbbot"], min_bot)
        new_list.append(row)

    # convert to a data frame
    line_item_df = aligned_rows_to_df(new_list)
    line_item_df["filename"] = filename
    return line_item_df


def predictions_to_df(submissions, doc_predictions, page_num=False):
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
            if page_num:
                pred_dict["page_num"].append(prediction["page_num"])
            if "confidence" in prediction:
                pred_dict["confidence"].append(prediction["confidence"][label])
            else:
                pred_dict["confidence"].append(1.0)
            pred_dict["filename"] = sub.input_filename

    pred_df = pd.DataFrame(pred_dict)
    return pred_df


def get_top_pred_df(pred_df, page_num=False):

    if page_num:
        groupby_cols = ["filename", "page_num", "label"]
    else:
        groupby_cols = ["filename", "label"]

    pred_highest_df = pred_df.loc[pred_df.groupby(groupby_cols)["confidence"].idxmax()]
    return pred_highest_df


def vert_to_horizontal(top_conf_df, page_num=False):
    """
    Assumes that df only contains one value per label
    """
    if page_num:
        cols = ["filename", "page_num", "label"]
    else:
        cols = ["filename", "label"]

    pivot_val_cols = ["text", "confidence"]
    dfs = []
    for pivot_val_col in pivot_val_cols:
        columns = cols + [pivot_val_col]
        subset_df = top_conf_df[columns].set_index(cols)
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
    for aligned_row in aligned_rows:
        df_row = defaultdict(str)
        max_count = 0
        for label, values in aligned_row.items():
            if len(values) > max_count:
                max_count = len(values)
        rows = [defaultdict(str) for i in range(max_count)]

        for label, values in aligned_row.items():
            # concatenate multiple values
            # TODO: need to investigate whether this is a correct approach
            for col, value in enumerate(values):
                rows[col][f"{label} text"] = str(value["text"])
                rows[col][f"{label} confidence"] = value["confidence"]
                rows[col]["page_num"] = value["page_num"]

        for row in rows:
            df_rows.append(row)

    return pd.DataFrame(df_rows)


def contains_added_text(predictions, fields):
    for pred in predictions:
        if (
            (pred["start"] == 0 and pred["end"] == 0)
            or (pred["start"] is None and pred["end"] is None)
        ) and pred["label"] in fields:
            return True
    return False


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


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(USAGE_STRING)
        sys.exit()

    configuration_path = sys.argv[1]
    if not os.path.exists(configuration_path):
        print(f"configuration file: {configuration_path} does not exist")
        sys.exit()

    config = ExportConfiguration.from_yaml(configuration_path)
    FIELD_CONFIG_FILEPATH = config.field_config_filepath
    if FIELD_CONFIG_FILEPATH:
        field_config_obj = FieldConfiguration.from_yaml(FIELD_CONFIG_FILEPATH)
        field_config = field_config_obj.field_config
    HOST = config.host
    API_TOKEN_PATH = config.api_token_path

    WORKFLOW_ID = config.workflow_id
    MODEL_NAME = config.model_name

    DOC_KEY_FIELDS = config.doc_key_fields
    PAGE_KEY_FIELDS = config.page_key_fields
    ROW_FIELDS = config.row_fields
    POST_PROCESSING = config.post_processing
    BATCH_SIZE = config.export_batch_size

    EXPORT_DIR = config.export_dir
    EXPORT_FILENAME = config.export_filename
    EXCEPTION_FILENAME = config.exception_filename
    DEBUG = config.debug
    STP = config.stp
    
    
    EXCEPTION_STATUS = "PENDING_ADMIN_REVIEW"
    COMPLETE_STATUS = "COMPLETE"

    indico_wrapper = IndicoWrapper(HOST, API_TOKEN_PATH)
    retrieved = config.retrieved
    post_review = not STP
    exception_ids = []
    exception_filenames = []
    complete_filenames = []
    sub_job = []
    result = []
    complete_revID = []
    exceptions_revID = []

    # To export COMPLETE submissions
    complete_submissions = indico_wrapper.get_submissions(
        WORKFLOW_ID, COMPLETE_STATUS, retrieved
    )

    total_submissions = len(complete_submissions)
    if BATCH_SIZE is None:
        BATCH_SIZE = total_submissions

    print(f"Starting processing of {total_submissions} submissions")
    if complete_submissions:
        batched_submissions = range(0, len(complete_submissions), BATCH_SIZE)
    else:
        batched_submissions = []
        
    for batch_start in batched_submissions:
        batch_end = batch_start + BATCH_SIZE
        submission_batch = complete_submissions[batch_start:batch_end]
        # FULL WORK FLOW
        full_dfs = []
        for submission in complete_submissions:
            page_infos, predictions = get_page_extractions(
                indico_wrapper, submission, MODEL_NAME, post_review=post_review
            )
            if predictions:
                # apply post processing functions
                if POST_PROCESSING:
                    inital_predictions = {MODEL_NAME: predictions}
                    reviewer = Reviewer(inital_predictions, MODEL_NAME, field_config)
                    reviewer.apply_reviews()
                    predictions = reviewer.get_updated_predictions()[MODEL_NAME]

                # first check for manually added preds, add them to exception queue
                if contains_added_text(predictions, ROW_FIELDS + PAGE_KEY_FIELDS):
                    exception_ids.append(int(submission.id))
                    exception_filenames.append(str(submission.input_filename))
                    if not DEBUG:
                        indico_wrapper.mark_retreived(submission)
                    continue

                tokens = merge_page_tokens(page_infos)
                add_page_number(predictions, tokens)
                doc_key_predictions = filter_preds(predictions, DOC_KEY_FIELDS)
                page_key_predictions = filter_preds(predictions, PAGE_KEY_FIELDS)
                row_predictions = filter_preds(predictions, ROW_FIELDS)

                # this may need it's own function/ better abstraction
                if doc_key_predictions:
                    key_preds_vert_df = predictions_to_df(
                        [submission], [doc_key_predictions]
                    )
                    top_conf_key_pred_df = get_top_pred_df(key_preds_vert_df)
                    key_pred_df = vert_to_horizontal(top_conf_key_pred_df)

                if page_key_predictions:
                    key_preds_vert_df = predictions_to_df(
                        [submission], [page_key_predictions], page_num=True
                    )
                    top_conf_key_pred_df = get_top_pred_df(
                        key_preds_vert_df, page_num=True
                    )
                    page_key_pred_df = vert_to_horizontal(
                        top_conf_key_pred_df, page_num=True
                    )

                if row_predictions:
                    line_item_df = align_rows(
                        row_predictions, tokens, submission.input_filename
                    )

                # TODO: this logic is gross
                if doc_key_predictions:
                    if row_predictions and page_key_predictions:
                        full_df = key_pred_df.merge(page_key_pred_df, how="outer")
                        full_df = full_df.merge(
                            line_item_df, on=["filename", "page_num"], how="outer"
                        )
                    else:
                        if row_predictions:
                            full_df = key_pred_df.merge(line_item_df, how="outer")
                        elif page_key_predictions:
                            full_df = key_pred_df.merge(page_key_pred_df)
                        else:
                            full_df = key_pred_df

                elif row_predictions:
                    if page_key_predictions:
                        full_df = line_item_df.merge(
                            page_key_pred_df, on=["filename", "page_num"], how="outer"
                        )
                    else:
                        full_df = line_item_df

                elif doc_key_predictions:
                    full_df = page_key_pred_df

                else:
                    continue
                full_dfs.append(full_df)

        if full_dfs:
            output_df = pd.concat(full_dfs)
            labels = DOC_KEY_FIELDS + PAGE_KEY_FIELDS + ROW_FIELDS
            col_order = ["filename"]
            pivot_val_cols = ["text", "confidence"]
            for label in labels:
                for pivot_val_col in pivot_val_cols:
                    col_order.append(f"{label} {pivot_val_col}")

            current_cols = set(output_df.columns)
            missing_cols = list(set(col_order).difference(current_cols))
            for missing_col in missing_cols:
                output_df[missing_col] = None

            doc_key_text_cols = [f"{col} text" for col in DOC_KEY_FIELDS]
            doc_key_conf_cols = [f"{col} confidence" for col in DOC_KEY_FIELDS]
            output_df.reset_index(drop=True, inplace=True)
            output_df[doc_key_text_cols] = output_df.groupby(["filename"], sort=False)[
                doc_key_text_cols
            ].apply(lambda x: x.ffill().bfill())
            output_df[doc_key_conf_cols] = output_df.groupby(["filename"], sort=False)[
                doc_key_conf_cols
            ].apply(lambda x: x.ffill().bfill())
            output_df = output_df[col_order]

            for cs in complete_submissions:
                complete_filenames.append(str(cs.input_filename))

            complete_filenames_df = pd.DataFrame(complete_filenames, columns=["filename"])
        
            for cs in complete_submissions:
                sub_job = (IndicoClient.call(SubmissionResult(cs.id, wait=True)))
                result = (IndicoClient.call(RetrieveStorageObject(sub_job.result)))
                complete_revID.append(result.get('reviewer_id'))
    
            complete_revID_df = pd.DataFrame(complete_revID, columns=["Reviewer ID"])
        
            reviewer_filename_df = pd.concat([complete_filenames_df, complete_revID_df], axis=1)
    
            output_df = pd.merge(reviewer_filename_df, output_df,  on='filename', how='outer')
            
            output_filepath = os.path.join(EXPORT_DIR, EXPORT_FILENAME)
            output_df.to_csv(output_filepath, index=False)
            print(f"Generated export {output_filepath}")
            total_processed = min(batch_end, total_submissions)
            print(f"Processed {total_processed}/ {total_submissions}")
            print("An export file has been generated")

            if not DEBUG:
                for sub in complete_submissions:
                    indico_wrapper.mark_retreived(sub)

        else:
            print("No COMPLETE submissions to generate export")

    EXCEPTION_STATUS = "PENDING_ADMIN_REVIEW"
    exception_submissions = indico_wrapper.get_submissions(
        WORKFLOW_ID, EXCEPTION_STATUS, retrieved
    )

    # Creating a DataFrame to store Exception Submission IDs and their corresponding filenames

    for es in exception_submissions:
        exception_ids.append(int(es.id))

    exception_ids_df = pd.DataFrame(exception_ids, columns=["Submission ID"])

    for es in exception_submissions:
        exception_filenames.append(str(es.input_filename))

    exception_filenames_df = pd.DataFrame(exception_filenames, columns=["File Name"])
    exceptions_df = pd.concat([exception_filenames_df, exception_ids_df], axis=1)
    for es in exception_submissions:
        sub_job = IndicoClient.call(SubmissionResult(es.id, wait=True))
        result = IndicoClient.call(RetrieveStorageObject(sub_job.result))
        exceptions_revID.append(result.get("reviewer_id"))

    exceptions_revID_df = pd.DataFrame(exceptions_revID, columns=["Reviewer ID"])
    exceptions_df = pd.concat(
        [exception_ids_df, exception_filenames_df, exceptions_revID_df], axis=1)

    if not DEBUG:
        for sub in exception_submissions:
            indico_wrapper.mark_retreived(sub)

    # Exporting Exception files and their Submission IDs as a CSV
    exception_filepath = os.path.join(EXPORT_DIR, EXCEPTION_FILENAME)
    exceptions_df.to_csv(exception_filepath, index=False)
    print(f"Generated exceptions {exception_filepath}")
    print("An exception file has been generated")
