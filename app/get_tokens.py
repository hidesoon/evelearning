import json


from esipy import App
from esipy import EsiClient
from esipy import EsiSecurity

app = App.create(url="https://esi.evetech.net/latest/swagger.json?datasource=tranquility")

# replace the redirect_uri, client_id and secret_key values
# with the values you get from the STEP 1 !
security = EsiSecurity(
    app=app,
    redirect_uri='your uri',
    client_id='your id',
    secret_key='your key',
)

# and the client object, replace the header user agent value with something reliable !
client = EsiClient(
    retry_requests=True,
    headers={'User-Agent': 'your app name'},
    security=security
)

# import scope

# with open ('scopes', 'rb') as fp:
#     scopeslist = pickle.load(fp)

sscp = app.root._Swagger__securityDefinitions['evesso']._SecurityScheme__scopes
scopeslist = list()
for i in sscp:
    scopeslist.append(i)

# this print a URL where we can log in
print(security.get_auth_uri(scopes=scopeslist,state='here'))
print("please copy your code and paste here\n")
authcode = input()

# YOUR_CODE is the code you got from Step 3. (do not forget quotes around it)
tokens = security.auth(authcode)
print(tokens)

# use the verify endpoint to know who we are
api_info = security.verify()

print(api_info)
# api_info contains data like this
# {
#   "Scopes": "esi-wallet.read_character_wallet.v1",
#   "ExpiresOn": "2017-07-14T21:09:20",
#   "TokenType": "Character",
#   "CharacterName": "SOME Char",
#   "IntellectualProperty": "EVE",
#   "CharacterOwnerHash": "4raef4rea8aferfa+E=",
#   "CharacterID": 123456789
# }

# now get the wallet data
op = app.op['get_characters_character_id_wallet'](
    character_id=api_info['CharacterID']
)
wallet = client.request(op)

# and to see the data behind, let's print it
print(wallet.data)

with open('tokens.txt', 'w') as outfile:
    json.dump(tokens, outfile)
