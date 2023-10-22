import json


def read_job_config(folder, job_name):
    f = open(f'{folder}{job_name}.json')
    content = json.load(f)
    f.close()
    return content
