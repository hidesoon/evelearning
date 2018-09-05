# %% get data page 1


op = app.op['get_corporations_corporation_id_contracts'](corporation_id=98185110)
token_status = is_tokens_expire(security)
if token_status is True:
    tokens, security = refresh_tokens(tokens, security)
    print("Checked, token updated")
op = app.op['get_corporations_corporation_id_contracts'](corporation_id=98185110)

try:
    res = client.request(op)

except Exception as e:
    print('Error:' + str(e) + "\n")
    nowstr = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    with open('log.text', 'a') as the_file:
        the_file.write(nowstr + ' Error: ' + str(res.status) + " " + str(e) + ' OP: ' + str(opcall) + "\n")

# check the error remain
e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
e_status = res.status

# reaction to error
if e_remain < 50:
    print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
if e_status == 420:
    time.sleep(e_reset)
# log the error message in local file
print('ESI status: {} you made {} error, remain: {}'.format(e_status, 100 - e_remain, e_remain))

df = pd.read_json(res.raw)

# %% get all pages

if res.status == 200:
    number_of_page = res.header['X-Pages'][0]

    for page in range(1, number_of_page):
        op = app.op['get_corporations_corporation_id_contracts'](corporation_id=98185110, page=page)

        try:
            res = client.request(op)

        except Exception as e:
            print('Error:' + str(e) + "\n")
            nowstr = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            with open('log.text', 'a') as the_file:
                the_file.write(nowstr + ' Error: ' + str(res.status) + " " + str(e) + ' OP: ' + str(opcall) + "\n")

        # check the error remain
        e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
        e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
        e_status = res.status

        # reaction to error
        if e_remain < 50:
            print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
        if e_status == 420:
            time.sleep(e_reset)
        # log the error message in local file
        print('ESI status: {} you made {} error, remain: {}'.format(e_status, 100 - e_remain, e_remain))

        df1 = pd.read_json(res.raw)
        df = df.append(df1, ignore_index=True)

# %% save to csv

opid = res._Response__op._Operation__operationId
nowstr = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
filename = opid + "_" + nowstr + ".csv"
df.to_csv(filename,encoding='utf_8_sig')