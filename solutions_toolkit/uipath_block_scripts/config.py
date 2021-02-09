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
        self.post_processing = self.get_key("POST_PROCESSING")
        
        self.retrieved = self.get_key("RETRIEVED")
        self.export_batch_size = self.get_key("EXPORT_BATCH_SIZE")
        self.doc_key_fields = self.get_list_key("DOC_KEY_FIELDS")
        self.page_key_fields = self.get_list_key("PAGE_KEY_FIELDS")
        self.row_fields = self.get_list_key("ROW_FIELDS")
        self.export_dir = self.get_key("EXPORT_DIR")
        self.stp = self.get_key("STP")
        self.field_config_filepath = self.get_key("FIELD_CONFIG_FILE")
        self.debug = self.get_key("DEBUG")
        self.export_filename = self.get_key("EXPORT_FILENAME")
        self.exception_filename = self.get_key("EXCEPTION_FILENAME")

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
