# %% get location list

filename = 'dt_locations.csv'

df_loc=pd.read_csv(filename).drop(columns='Unnamed: 0')


df_structure=df_loc.loc[df_loc['pos_tpye'] == 'structure']

list_structure=df_structure['location_id'].tolist()

list_structure_accessable=list()
# %% get data page 1


for item in list_structure:
    print(item)
    op = app.op['get_markets_structures_structure_id'](structure_id=item)
    token_status = is_tokens_expire(security)
    if token_status is True:
        tokens, security = refresh_tokens(tokens, security)
        print("Checked, token updated")
    op = app.op['get_markets_structures_structure_id'](structure_id=item)

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
    # if e_status == 420:
        time.sleep(e_reset)
    if e_status == 403:
        print(res.raw)
        continue
    # log the error message in local file
    print('ESI status: {} you made {} error, remain: {}'.format(e_status, 100 - e_remain, e_remain))

    df = pd.read_json(res.raw)

    # get all pages

    if res.status == 200:
        list_structure_accessable.append(item)
        number_of_page = res.header['X-Pages'][0]

        for page in range(1, number_of_page):
            op = app.op['get_markets_structures_structure_id'](structure_id=item, page=page)

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
            if e_remain < 30:
                print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
            if e_status == 420:
                time.sleep(e_reset)
            # log the error message in local file
            print('ESI status: {} you made {} error, remain: {}'.format(e_status, 100 - e_remain, e_remain))

            df1 = pd.read_json(res.raw)
            df = df.append(df1, ignore_index=True)

    try:
        dfs
    except NameError:
        dfs = df
    else:
        dfs = dfs.append(df, ignore_index=True)

# %% merge
df_structure_accessable=df_loc.loc[df_loc['location_id'].isin(list_structure_accessable)]



filename = 'dt_structure_orders_winter.csv'
dfs.to_csv(filename, encoding='utf_8_sig')

filename_history = 'dt_structure_orders_winter_rec.csv'

try:
    df_s_orders = pd.read_csv(filename_history)
except Exception as e:
    print('Error:' + str(e) + "\n")
    dfs.to_csv(filename_history, encoding='utf_8_sig')
    df_s_orders = dfs

df_s_orders = df_s_orders.drop(columns='Unnamed: 0')

rr0, cc0 = df_s_orders.shape

df_merged = dfs.append(df_s_orders, ignore_index=True)

df_merged = df_merged.drop_duplicates(subset=['order_id'])
rr1, cc1 = df_merged.shape

print('There are {} rows in store, added {} new rows'.format(rr0, rr1 - rr0))

df_merged.to_csv(filename_history, encoding='utf_8_sig')
print('structure order mega saved')
