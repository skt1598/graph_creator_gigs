from dotenv import load_dotenv
import requests
import json
import os
load_dotenv()

def set_headers():
    headers = {
    'Content-Type': 'application/json'
    }
    return headers


def get_tags(body):
    nlp_url = os.getenv("NLP_URL")
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", nlp_url, headers=headers, data = body)
    tags = []
    # print("response.status_code", response.status_code)
    if response.status_code == 200:
        tags = json.loads(response.content)    
        print("\ntags from api", tags)
    return tags

def similar_skills(body):
    headers = set_headers()
    fiver_base_url = os.getenv('FIVER_BASE_URL')
    similar_skills = requests.request("POST", fiver_base_url+"similar_skills", headers=headers, data = json.dumps(body))
    if similar_skills.status_code == 200:
        skills = json.loads(similar_skills.content)
        return skills
    else:
        return False

def same_skills(body):
    headers = set_headers()
    fiver_base_url = os.getenv('FIVER_BASE_URL')
    same_skills = requests.request("POST", fiver_base_url+"same_skills", headers=headers, data = json.dumps(body))
    if same_skills.status_code == 200:
        skills = json.loads(same_skills.content)
        if body['user_skill'] in skills and len(skills[body['user_skill']]) > 0:
            return skills[body['user_skill']]
    return False

def sub_domain(body):
    headers = set_headers()
    fiver_base_url = os.getenv('FIVER_BASE_URL')
    res = requests.request("POST", fiver_base_url+"fiver_subdomain", headers=headers, data = json.dumps(body))
    if res.status_code == 200:
        sub_domain = json.loads(res.content)
        if len(sub_domain.items()) > 0:
            (key, val), = sub_domain.items()
#             tags = {k: v for k, v in sorted(sub_domain[key].items(), key=lambda item: item[1], reverse= True)[:10]}
            return val
    return False