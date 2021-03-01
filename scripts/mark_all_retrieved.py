from solutions_toolkit.indico_wrapper import IndicoWrapper

host = "cush.indico.domains"
api_token_path = "C:\\Users\\Rohan\\Documents\\indico_dev_api_token.txt"
workflow_id = 206

indico_wrapper = IndicoWrapper(host, api_token_path)
complete_submissions = indico_wrapper.get_submissions(workflow_id, "COMPLETE", retrieved_flag=False)

for submission in complete_submissions:
    indico_wrapper.mark_retreived(submission)
