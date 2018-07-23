# identify the exist of tokens
import datetime
import json
from pprint import pprint

import pandas as pd
import numpy as np
import matplotlib
from dateutil import parser
from esipy import App
from esipy import EsiClient
from esipy import EsiSecurity

# TODO: should design a function to check whether new items created in the server
# TODO: Should have a better way rather than run everything in python console. GUI needed???

def check_token_available():
    try:
        with open('tokens.txt') as infile:
            tokens = json.load(infile)
            print("Tokens loaded\n")
            return tokens
    except Exception as e:
        print('Error:' + str(e) + "\n")
        print("No token!")
        return None


def check_app_key():
    try:
        with open('appkey.txt') as infile:
            appkey = json.load(infile)
            print("App key loaded\n")
            return appkey
    except Exception as e:
        print('Error:' + str(e) + "\n")
        print("No App key!")
        return None


def cynoup(app_key, appname):
    app = App.create(url="https://esi.tech.ccp.is/latest/swagger.json?datasource=tranquility")

    # replace the redirect_uri, client_id and secret_key values
    # with the values you get from the STEP 1 !
    security = EsiSecurity(
        app=app,
        redirect_uri=app_key['redirect_uri'],
        client_id=app_key['client_id'],
        secret_key=app_key['secret_key'],
    )

    # and the client object, replace the header user agent value with something reliable !
    client = EsiClient(
        retry_requests=True,
        headers={'User-Agent': appname},
        security=security
    )
    return app, security, client


def is_tokens_expire(security):
    api_info = security.verify()
    expiretime = api_info.get('ExpiresOn')

    # UTC time now
    dt_now = datetime.datetime.utcnow()

    dt_due = parser.parse(expiretime)
    due = dt_due - dt_now
    if dt_now < dt_due:
        print("Not yet Experie" "\n")
        return False
    else:
        print("Warning: token already experied\n")
        return True


def refresh_tokens(tokens_old, security):
    ref_token = tokens_old['refresh_token']

    # to update the security object,
    security.update_token({
        'access_token': '',  # leave this empty
        'expires_in': -1,  # seconds until expiry, so we force refresh anyway
        'refresh_token': ref_token
    })

    tokens = security.refresh()
    with open('tokens.txt', 'w') as outfile:
        json.dump(tokens, outfile)

    api_info = security.verify()
    pprint(api_info)
    return tokens, security


def getdata(app, client, security, opcall, personal):
    # generate the operation tuple
    # the parameters given are the actual parameters the endpoint requires
    api_info = security.verify()
    # 'get_characters_character_id_skillqueue'
    if personal == 1:
        op = app.op[opcall](character_id=api_info['CharacterID'])
    else:
        op = app.op[opcall]
    res = client.request(op)
    return res


def savelocal(res):
    df = pd.read_json(res.raw)
    opid = res._Response__op._Operation__operationId
    nowstr = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    filename = opid + "_" + nowstr + ".csv"
    df.to_csv(filename)
    return df


# if __name__ == "__main__":
tokens = check_token_available()
app_key = check_app_key()
appname = 'HIDETHEARTIST'  # please change to your own appname
app, security, client = cynoup(app_key=app_key, appname=appname)
tokens, security = refresh_tokens(tokens, security)

    # if is_tokens_expire(security):
    #     tokens, security = refresh_tokens(tokens, security)
    #     print("token updated")

    # opcall = 'get_characters_character_id_skillqueue'
    #
    # res = getdata(app, client, security, opcall, personal=1)

    # df = savelocal(res)
