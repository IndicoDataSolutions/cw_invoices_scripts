def reject_by_confidence(prediction, label, conf_threshold=0.50): 
    if prediction["confidence"][label] < conf_threshold:
        prediction["rejected"] = True
    return prediction


def accept_by_confidence(prediction, label, conf_threshold=0.98):
    if prediction["confidence"][label] > conf_threshold:
        prediction["accepted"] = True


def accept_all_by_confidence(predictions, label, conf_threshold=0.98):
    pred_values = set()
    for pred in predictions:
        if pred["confidence"][label] < conf_threshold:
            return predictions
        pred_values.add(pred["text"])
    if len(pred_values) == 1:
        for pred in predictions:
            pred["acccepted"] = True
    return predictions


def reject_by_character_length(prediction, length_threshold=3):
    if len(prediction["text"]) < length_threshold:
        prediction["rejected"] = True
    return prediction
