import requests
from qualtrics_keys import token, data_center, survey_id


# https://api.qualtrics.com/6b00592b9c013-start-response-export
# Start a response export


def start_response_export(token, data_center, survey_id, format = "csv", numeric=False):
    
    start_url = "https://{data_center}.qualtrics.com/API/v3/surveys/{survey_id}/export-responses".format(data_center=data_center, survey_id=survey_id)

    
    if format == "csv":
        payload = { 
            "format": "csv" ,
            "compress": False,
            "useLabels": not numeric,
            "surveyMetadataIds": ["duration", "finished", "progress"]
            }
    elif format == "json":
        payload = { 
            "format": "json" ,
            "compress": False,
            "surveyMetadataIds": ["duration", "finished", "progress"]
            }
        
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-API-TOKEN": token
    }

    response = requests.post(start_url, json=payload, headers=headers)


    if response.status_code != 200:
        print("Error: ", response.status_code)
        print(response.text)
        return False, None, None
    else:
        start_result = response.json()
        export_progress_id = start_result['result']['progressId']
        print("Export progress ID: ", export_progress_id)
        
    return True, export_progress_id, start_result

from typing import Literal

def check_response_export(token, data_center, survey_id, export_progress_id):
    check_url = "https://{data_center}.qualtrics.com/API/v3/surveys/{survey_id}/export-responses/{export_progress_id}".format(data_center=data_center, survey_id=survey_id, export_progress_id = export_progress_id)

    
    headers = {
        "Accept": "application/json",
        "X-API-TOKEN": token
    }

    response = requests.get(check_url, headers=headers)

    if response.status_code != 200:
        print("Error: ", response.status_code)
        print(response.text)
        return False
    else:
        check_result = response.json() 
        if 'result' in check_result:
            if 'status' in check_result['result']:
                if check_result['result']['status'] == 'complete':
                    print("fileId: ", check_result['result']['fileId'])
                    return check_result['result']['fileId']
                elif 'percentComplete' in check_result['result']:
                    print("Percent complete: ", check_result['result']['percentComplete'])
                    return False
            
    return False


def download_response_export(token, data_center, survey_id, file_id):
    requestDownloadUrl = "https://{data_center}.qualtrics.com/API/v3/surveys/{survey_id}/export-responses/{file_id}/file".format(data_center=data_center, survey_id=survey_id, file_id = file_id)
    headers = {
        "Accept": "application/json",
        "X-API-TOKEN": token
    }
    response = requests.request("GET", requestDownloadUrl, headers=headers, stream=True)

    if response.status_code != 200:
        print("Error in download: ", response.status_code)

    return response

import time
def exponential_backoff(func, *args, **kwargs):
    """
    Retry a function with exponential backoff.
    """
    max_retries = 5
    for i in range(max_retries):
        out = func(*args, **kwargs)
        if out == False:
            print(f"Error: Retrying in {2 ** i} seconds...")
            time.sleep(2 ** i)
        else:
            return out
    raise Exception("Max retries exceeded")


from io import StringIO
def get_survey(token, data_center, survey_id, format="csv", numeric=False):
    """
    Get survey responses from Qualtrics API.
    """
    # Start the response export
    success, export_progress_id, start_result = start_response_export(token, data_center, survey_id, format=format, numeric=numeric)
    
    
    if not success:
        print("Failed to start response export.")
        return

    # Check the response export status
    file_id = check_response_export(token, data_center, survey_id, export_progress_id)
    max_tries = 3
    while file_id == False and max_tries > 0:
        print("Waiting for response export to complete...")
        file_id = exponential_backoff(check_response_export, token, data_center, survey_id, export_progress_id)
        max_tries -= 1
    
    
    if file_id == False:
        print("Failed to get response export.")
        return
        # Download the response export
    download_result = download_response_export(token, data_center, survey_id, file_id)
    if download_result.status_code != 200:
        print("Error in download: ", download_result.status_code)
        return
    return download_result.content.decode('utf-8')

