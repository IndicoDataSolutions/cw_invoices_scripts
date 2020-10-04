# Cushman Wakefield Invoice Use Case Scripts

Scripts to handle ETL for CW Invoice Use Case

### workflow_upload.py

Upload invoices to model workflow.  Note this is where STP logic should be
handled in the future

### generate_export.py

Query for COMPLETE submissions from the invoice model workflow and generate a
csv export for further downstream CW processes
