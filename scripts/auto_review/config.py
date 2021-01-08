FIELD_CONFIG = {
    "Currency": [
        {
            "function": "accept_by_confidence",
            "kwargs": {
                "label": "Currency",
                "conf_threshold": 0.98
            },
            "prediction_set": "single",
        },
        {
            "function": "reject_by_confidence",
            "kwargs": {
                "label": "Currency",
                "conf_threshold": 0.5
            },
            "prediction_set": "single",
        }
    ],
    "Supplier Name": [
        {
            "function": "accept_all_by_confidence",
            "kwargs": {
                "label": "Supplier Name",
                "conf_threshold": 0.98
            },
            "prediction_set": "all",
        },
        {
            "function": "reject_by_confidence",
            "kwargs": {
                "label": "Supplier Name",
                "conf_threshold": 0.5
            },
            "prediction_set": "single",
        },
        {
            "function": "reject_by_character_length",
            "kwargs": {
                "length_threshold": 3
            },
            "prediction_set": "single",
        },
    ]
}
