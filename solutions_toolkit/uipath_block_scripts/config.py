from solutions_toolkit.configuration import Configuration


# TODO: need to do config checks for error catching
class ExportConfiguration(Configuration):
    def __init__(self, config):
        self.config = config
        self.host = self.get_key("HOST")
        self.api_token_path = self.get_key("API_TOKEN_PATH")
        self.workflow_id = self.get_key("WORKFLOW_ID")
        self.model_name = self.get_key("MODEL_NAME")
        self.batch_size = self.get_key("BATCH_SIZE")
        self.invoice_input_dir = self.get_key("INVOICE_INPUT_DIR")
        self.uploaded_dir = self.get_key("UPLOADED_DIR")
        self.timeout = self.get_key("TIMEOUT")
        self.wait = self.get_key("WAIT")

    # TODO: this check needs to go layers deep
    def get_key(self, key):
        try:
            return self.config[key]
        except KeyError as e:
            print(f"Missing configuration for {key}!!!!!")
            raise e
