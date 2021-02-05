from solutions_toolkit.configuration import Configuration


# TODO: need to do config checks for error catching
class ExportConfiguration(Configuration):
    def __init__(self, config):
        self.config = config
        self.host = self.get_key("HOST")
        self.api_token_path = self.get_key("API_TOKEN_PATH")
        self.workflow_id = self.get_key("WORKFLOW_ID")
        self.model_name = self.get_key("MODEL_NAME")
        self.upload_batch_size = self.get_key("UPLOAD_BATCH_SIZE")
        self.document_input_dir = self.get_key("DOCUMENT_INPUT_DIR")
        self.uploaded_dir = self.get_key("UPLOADED_DIR")
        self.timeout = self.get_key("TIMEOUT")
        self.wait = self.get_key("WAIT")

        self.export_batch_size = self.get_list_key("EXPORT_BATCH_SIZE")
        self.doc_key_fields = self.get_list_key("DOC_KEY_FIELDS")
        self.page_key_fields = self.get_list_key("PAGE_KEY_FIELDS")
        self.row_fields = self.get_key("ROW_FIELDS")
        self.export_dir = self.get_key("EXPORT_DIR")
        self.stp = self.get_key("STP")
        self.debug = self.get_key("DEBUG")
        
    # TODO: this check needs to go layers deep
    def get_key(self, key):
        try:
            return self.config[key]
        except KeyError as e:
            print(f"Missing configuration for {key}!!!!!")
            raise e

    def get_list_key(self, key):
        try:
            if self.config[key]:
                return self.config[key]
            else:
                return []
        except KeyError as e:
            print(f"Missing configuration for {key}!!!!!")
            raise e
