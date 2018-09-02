import requests
url = "https://evepraisal.com/appraisal"
payload = {
    'raw_textarea': 'Tritanium 1',
    'market': 'jita',
}
r = requests.post(url, params=payload)
appraisal_id = r.headers['X-Appraisal-Id']
appraisal_url = "https://evepraisal.com/a/{}.json".format(appraisal_id)
result = requests.get(appraisal_url).json()
print(result)