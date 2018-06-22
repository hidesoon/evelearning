# find the scope of ESI
import requests

esi_latest_url = "https://esi.tech.ccp.is/latest/swagger.json"

r = requests.get(esi_latest_url)
with open('esi_latest_url.txt', 'w') as outfile:
    json.dump(r.json(), outfile)

scopeslist = list()

for i in r.json()['securityDefinitions']['evesso']['scopes']:
    scopeslist.append(i)
print(scopeslist)

import pickle

with open('scopes', 'wb') as fp:
    pickle.dump(scopeslist, fp)

# with open ('scopes', 'rb') as fp:
#     itemlist = pickle.load(fp)