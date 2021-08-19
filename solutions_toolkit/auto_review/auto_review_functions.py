import re

ACCEPTED = "accepted"
REJECTED = "rejected"


def reject_by_confidence(prediction, label="asdf", conf_threshold=0.50):
    if prediction.get(REJECTED):
        return prediction
    if prediction["confidence"][label] < conf_threshold:
        prediction[REJECTED] = True
        _ = prediction.pop(ACCEPTED, None)
    return prediction


def remove_by_confidence(predictions, label="asdf", conf_threshold=0.50):
    updated_predictions = []
    for prediction in predictions:
        if prediction["confidence"][label] > conf_threshold:
            updated_predictions.append(prediction)
    return updated_predictions


def accept_by_confidence(prediction, label="asdf", conf_threshold=0.98):
    if prediction.get(REJECTED) is None:
        if prediction["confidence"][label] > conf_threshold:
            prediction[ACCEPTED] = True
    return prediction


def accept_all_by_confidence(predictions, label="asdf", conf_threshold=0.98):
    pred_values = set()

    for pred in predictions:
        if pred.get(REJECTED) is None:
            if pred["confidence"][label] > conf_threshold:
                pred_values.add(pred["text"])

    if len(pred_values) == 1:
        text = pred_values.pop()
        for pred in predictions:
            if pred["text"] == text:
                if not pred["confidence"][label] > conf_threshold:
                    return predictions

        for pred in predictions:
            if pred["text"] == text:
                pred[ACCEPTED] = True
    return predictions


def reject_by_min_character_length(prediction, min_length_threshold=3):
    if len(prediction["text"]) < min_length_threshold:
        prediction[REJECTED] = True
    return prediction


def reject_by_max_character_length(prediction, max_length_threshold=10):
    if len(prediction["text"]) > max_length_threshold:
        prediction[REJECTED] = True
    return prediction


def split_merged_values(predictions, newline_only=False):
    updated_predictions = []
    for pred in predictions:
        merged_text = pred["text"]
        start = pred["start"]
        if newline_only:
            split_text = re.split(r"(\n)", merged_text)
        else:
            split_text = re.split(r"(\s)", merged_text)
        if len(split_text) == 1 or pred.get(REJECTED):
            updated_predictions.append(pred)
            continue

        current_start = start
        for text in split_text:
            str_len = len(text)
            if str_len == 0:
                continue
            if re.match(r"\s", text):
                current_start += 1
                continue

            split_value_start = current_start
            split_value_end = split_value_start + str_len
            current_start = split_value_end
            split_val_pred_dict = {
                "text": text,
                "start": split_value_start,
                "end": split_value_end,
                "label": pred["label"],
                "confidence": pred["confidence"],
            }
            updated_predictions.append(split_val_pred_dict)
    return updated_predictions

def fix_dates(date):
    backup_regex = r"^(?P<month>\d)[.\/1i](?P<day>\d{1,2})[.\/1i](?P<year>\d\d\d\d)$"
    main_regex = r"^(?P<month>\d{1,2})[.\/1i](?P<day>\d{1,2})[.\/1i](?P<year>\d\d\d\d)$"
    date_match = re.search(main_regex, date)
    if date_match:
        backup_match = re.search(backup_regex, date)
        # backup match matches something but it's not the same as main match it's because it's something ambiguous like 1111/2020
        if backup_match is None or (backup_match.group("month") == date_match.group("month") and backup_match.group("day") == date_match.group("day")):
            year = date_match.group('year')
            month = date_match.group('month')
            day = date_match.group('day')
            date = f"{month}/{day}/{year}"
    return date

def review_issue_dates(predictions):
    for pred in predictions:
        correct_date = fix_dates(pred["text"])
        if pred["text"] != correct_date:
            pred["text"] = correct_date
    return predictions
