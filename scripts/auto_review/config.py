FIELD_CONFIG = {
    "Currency": [
        {
            "function": "accept_by_confidence",
            "kwargs": {
                "conf_threshold": 0.98
            },
            "prediction_set": "single",
            "label_required": True,
        },
        {
            "function": "reject_by_confidence",
            "kwargs": {
                "conf_threshold": 0.5
            },
            "prediction_set": "single",
            "label_required": True,
        }
    ],
    "Supplier Name": [
        {
            "function": "accept_all_by_confidence",
            "kwargs": {
                "conf_threshold": 0.98
            },
            "prediction_set": "all",
            "label_required": True,
        },
        {
            "function": "reject_by_confidence",
            "kwargs": {
                "conf_threshold": 0.5
            },
            "prediction_set": "single",
            "label_required": True,
        },
        {
            "function": "reject_by_character_length",
            "kwargs": {
                "length_threshold": 3
            },
            "prediction_set": "single",
            "label_required": False,
        },
    ]
}
