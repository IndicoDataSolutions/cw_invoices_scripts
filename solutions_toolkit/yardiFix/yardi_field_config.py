"""
Field config configures the auto review functions that will apply to each class
in a document's prediction

It has the srtructure
{
    CLASS_NAME: [
        {
            "function": FUNCTION NAME,
            "kwargs": {FUCNTION KEY WORDS},
            "prediction_set": "single" or "all" ----> This determines whether the function works on the full label set or one at a time
        }
        .
        .
        .
    ]
}

Note that the functions you apply need to have an appropriate mapping in
the REVIEWERS dictionary in reviewer.py
"""

FIELD_CONFIG = {
    "Check Amount": [
        {
            "function": "split_merged_values",
            "kwargs": {},
            "prediction_set": "all",
        },
    ],
    "Check Date": [
        {
            "function": "split_merged_values",
            "kwargs": {},
            "prediction_set": "all",
        },
    ],
    "Check Number": [
        {
            "function": "split_merged_values",
            "kwargs": {},
            "prediction_set": "all",
        },
    ],
    "Deposit Amount": [
        {
            "function": "split_merged_values",
            "kwargs": {},
            "prediction_set": "all",
        },
    ],
    "Deposit Date": [
        {
            "function": "split_merged_values",
            "kwargs": {},
            "prediction_set": "all",
        },
    ],
    "Withdrawal Amount": [
        {
            "function": "split_merged_values",
            "kwargs": {},
            "prediction_set": "all",
        },
    ],
    "Withdrawal Date": [
        {
            "function": "split_merged_values",
            "kwargs": {},
            "prediction_set": "all",
        },
    ],
}
