# %% get data page 1
op_name = 'get_contracts_public_region_id'

region_ids = [10000012,  # curse
              10000005,  # Detorid
              10000061,  # Tenerifis
              10000009,  # Insmother
              10000025,  # Immensea
              10000006,  # Wicked Creek
              10000008,  # Scalding Pass
              ]
try:
    del df
    del dfs
except:
    print('cleaned')

for item in region_ids:
    print(item)
    op = app.op[op_name](region_id=item)
    token_status = is_tokens_expire(security)
    if token_status is True:
        tokens, security = refresh_tokens(tokens, security)
        print("Checked, token updated")
    op = app.op[op_name](region_id=item)

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
    if e_status == 403:
        print(res.raw)
        continue
    # log the error message in local file
    print('ESI status: {} you made {} error, remain: {}'.format(e_status, 100 - e_remain, e_remain))

    df = pd.read_json(res.raw)

    # get all pages

    if res.status == 200:
        number_of_page = res.header['X-Pages'][0]
        if number_of_page > 1:

            for page in range(1, number_of_page):
                op = app.op[op_name](region_id=item, page=page)
                print(page + 1)

                try:
                    res = client.request(op)

                except Exception as e:
                    print('Error:' + str(e) + "\n")
                    nowstr = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                    with open('log.text', 'a') as the_file:
                        the_file.write(
                            nowstr + ' Error: ' + str(res.status) + " " + str(e) + ' OP: ' + str(opcall) + "\n")

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
                df = df.append(df1, ignore_index=True, sort=False)

    try:
        dfs
    except NameError:
        dfs = df
        dfs['region_id'] = item
    else:
        df['region_id'] = item
        dfs = dfs.append(df, ignore_index=True, sort=False)

dfs = dfs.drop_duplicates(subset=['contract_id'])

df_contracts=dfs

# %% read & write

filename = 'dt_pub_contract_winter.csv'
dfs.to_csv(filename, encoding='utf_8_sig')

# %% get contract items
list_contract=df_contracts['contract_id'].tolist()

op_name = 'get_contracts_public_items_contract_id'


for item in list_contract:
    # print(item)
    op = app.op[op_name](contract_id=item)
    token_status = is_tokens_expire(security)
    if token_status is True:
        tokens, security = refresh_tokens(tokens, security)
        print("Checked, token updated")
    op = app.op[op_name](contract_id=item)

    try:
        res = client.request(op)

    except Exception as e:
        print('Error:' + str(e) + "\n")
        # nowstr = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        # with open('log.text', 'a') as the_file:
        #     the_file.write(nowstr + ' Error: ' + str(res.status) + " " + str(e) + ' OP: ' + str(opcall) + "\n")

    # check the error remain
    e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
    e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
    e_status = res.status

    # reaction to error
    if e_remain < 50:
        print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
    if e_status == 420:
        time.sleep(e_reset)
    if e_status != 200:
        print(item)
        print(e_status,res.raw)

        continue
    # log the error message in local file
    # print('ESI status: {} you made {} error, remain: {}'.format(e_status, 100 - e_remain, e_remain))

    df_contract_item = pd.read_json(res.raw)

    # get all pages

    if res.status == 200:
        number_of_page = res.header['X-Pages'][0]
        if number_of_page > 1:

            for page in range(1, number_of_page):
                op = app.op[op_name](contract_id=item, page=page)
                print(page + 1)

                try:
                    res = client.request(op)

                except Exception as e:
                    print('Error:' + str(e) + "\n")


                # check the error remain
                e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                e_status = res.status

                # reaction to error
                if e_remain < 30:
                    print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                    time.sleep(e_reset)
                if e_status != 200:
                    print(e_status, res.raw)
                    continue
                # log the error message in local file
                # print('ESI status: {} you made {} error, remain: {}'.format(e_status, 100 - e_remain, e_remain))

                df1 = pd.read_json(res.raw)
                df_contract_item = df_contract_item.append(df1, ignore_index=True, sort=False)
    else:
        print(e_status, res.raw)
        continue

    try:
        df_contract_items
    except NameError:
        df_contract_items = df_contract_item
        df_contract_items['contract_id'] = item
    else:
        df_contract_item['contract_id'] = item
        df_contract_items = df_contract_items.append(df_contract_item, ignore_index=True, sort=False)



filename = 'dt_pub_contract_winter_detail.csv'
df_contract_items.to_csv(filename, encoding='utf_8_sig')

# %% extract location

location_list = dfs['start_location_id'].drop_duplicates().tolist()

try:
    del df
    del dfs
except:
    print('cleaned')

for location_id in location_list:
    if location_id < 1000000000000:
        endpoint = 'get_universe_stations_station_id'

        op = app.op[endpoint](station_id=location_id)
        token_status = is_tokens_expire(security)
        if token_status is True:
            tokens, security = refresh_tokens(tokens, security)
            print("Checked, token updated")
        op = app.op[endpoint](station_id=location_id)

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
        if e_status == 403:
            print(res.raw)
            continue
        # log the error message in local file
        print('ESI status: {} you made {} error, remain: {}'.format(e_status, 100 - e_remain, e_remain))

        df = pd.io.json.json_normalize(json.loads(res.raw))
        df['pos_tpye'] = 'station'
        df['location_id'] = location_id
    else:
        endpoint = 'get_universe_structures_structure_id'

        op = app.op[endpoint](structure_id=location_id)
        token_status = is_tokens_expire(security)
        if token_status is True:
            tokens, security = refresh_tokens(tokens, security)
            print("Checked, token updated")
        op = app.op[endpoint](structure_id=location_id)

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
        if e_status == 403:
            print(res.raw)
            continue
        # log the error message in local file
        print('ESI status: {} you made {} error, remain: {}'.format(e_status, 100 - e_remain, e_remain))

        df = pd.io.json.json_normalize(json.loads(res.raw))
        df['pos_tpye'] = 'structure'
        df['location_id'] = location_id
    try:
        dfs
    except NameError:
        dfs = df
    else:
        dfs = dfs.append(df, ignore_index=True)

# %% read & write

filename = 'dt_locations.csv'
dfs.to_csv(filename, encoding='utf_8_sig')
