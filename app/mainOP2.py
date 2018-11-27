import concurrent
import time
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor

# import grequests as grequests
from requests_futures.sessions import FuturesSession
from termcolor import colored

import psycopg2
import json
import pandas as pd
import datetime

import requests
from esipy import App
from esipy import EsiClient
from esipy import EsiSecurity


def cynoup(app_key, appname):
    app = App.create(url="https://esi.tech.ccp.is/latest/swagger.json?datasource=tranquility")

    # replace the redirect_uri, client_id and secret_key values
    # with the values you get from the STEP 1 !
    security = EsiSecurity(
        app=app,
        redirect_uri=app_key['redirect_uri'],
        client_id=app_key['client_id'],
        headers={'User-Agent': appname},
        secret_key=app_key['secret_key'],
    )
    # and the client object, replace the header user agent value with something reliable !
    client = EsiClient(
        retry_requests=True,
        headers={'User-Agent': appname},
        security=security
    )
    print(colored(' >>>CYNO UP<<< ', 'green'))
    return app, security, client


def refresh_tokens(tokens_old, security):
    ref_token = tokens_old['refresh_token']

    # to update the security object,
    security.update_token({
        'access_token': '',  # leave this empty
        'expires_in': -1,  # seconds until expiry, so we force refresh anyway
        'refresh_token': ref_token
    })

    tokens = security.refresh()
    # need update

    print("Tokens refreshed\n ")
    # pprint(api_info)
    return tokens, security


def countdown(new, old):
    duration = new - old
    secs = duration.total_seconds()
    hh = secs // 3600
    mm = (secs % 3600) // 60
    ss = secs % 60
    print('Passed: {} h {} m {} s'.format(hh, mm, ss))


def add_location(location_list):
    # try:
    #     del df
    #     del dfs
    #
    # except:
    #     pass
    stamp1 = datetime.datetime.now(datetime.timezone.utc)
    conn, c = lighter()
    sql = 'SELECT DISTINCT location_id FROM universe_stations_temp'
    c.execute(sql)
    exlist = [x[0] for x in c.fetchall()]

    location_new = []
    for x in location_list:
        if x not in exlist:
            location_new.append(x)

    if len(location_new) > 0:

        print('\nfound new locations')

        for location_id in location_new:
            if location_id < 1000000000000:
                endpoint = 'get_universe_stations_station_id'

                op = app.op[endpoint](station_id=location_id)

                for i in range(0, 2):
                    while True:
                        try:
                            res = client.request(op)
                        except Exception as e:
                            print('Error:' + str(e) + "\n")

                            # check the error remain
                            e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                            e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                            e_status = res.status

                            # reaction to error
                            if e_remain < 50:
                                print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                                time.sleep(e_reset)
                                print('sleep {}s'.format(e_reset))
                            if e_status == 403:
                                print(res.raw)
                            continue
                        break
                if res.status == 200:
                    df = pd.io.json.json_normalize(json.loads(res.raw))
                    df['pos_tpye'] = 'station'
                    df['location_id'] = location_id
                else:
                    print(res.status, res.raw)
                    # check the error remain
                    e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                    e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                    e_status = res.status

                    # reaction to error
                    if e_remain < 50:
                        print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                        time.sleep(e_reset)
                        print('sleep {}s'.format(e_reset))
                    continue
            else:
                endpoint = 'get_universe_structures_structure_id'

                op = app.op[endpoint](structure_id=location_id)
                for i in range(0, 2):
                    while True:
                        try:
                            res = client.request(op)
                        except Exception as e:
                            print('Error:' + str(e) + "\n")

                            # check the error remain
                            e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                            e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                            e_status = res.status

                            # reaction to error
                            if e_remain < 50:
                                print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                                time.sleep(e_reset)
                                print('sleep {}s'.format(e_reset))
                            if e_status == 403:
                                print(res.raw)
                            continue
                        break

                if res.status == 200:
                    df = pd.io.json.json_normalize(json.loads(res.raw))
                    df['pos_tpye'] = 'structure'
                    df['location_id'] = location_id
                else:
                    print(res.status, res.raw)
                    # check the error remain
                    e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                    e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                    e_status = res.status

                    # reaction to error
                    if e_remain < 50:
                        print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                        time.sleep(e_reset)
                        print('sleep {}s'.format(e_reset))
                    continue
            try:
                dfs
            except NameError:
                dfs = df
            else:
                dfs = dfs.append(df, ignore_index=True, sort=False)

        stamp2 = datetime.datetime.now(datetime.timezone.utc)
        print('ESI done')
        countdown(stamp2, stamp1)

        #  read & write

        dict = dfs.to_dict(orient='records')

        conn, c = lighter()

        tablename = 'universe_stations_temp'
        c.execute('select count(*) from %s' % (tablename))
        before = c.fetchone()[0]

        # c.execute('TRUNCATE only %s' % tablename)
        # conn.commit()
        now = datetime.datetime.now(datetime.timezone.utc)

        for row in dict:

            try:
                row['owner']
            except:
                row['owner'] = None

            try:
                row['owner_id']
            except:
                row['owner_id'] = None

            try:
                row['max_dockable_ship_volume']
            except:
                row['max_dockable_ship_volume'] = None
            try:
                row['office_rental_cost']
            except:
                row['office_rental_cost'] = None

            try:
                row['race_id']
            except:
                row['race_id'] = None

            try:
                row['reprocessing_efficiency']
            except:
                row['reprocessing_efficiency'] = None

            try:
                row['reprocessing_stations_take']
            except:
                row['reprocessing_stations_take'] = None

            try:
                row['services']
            except:
                row['services'] = None

            # tag_date = 'date_issued'
            # if isinstance(row[tag_date], datetime.date) is False:  # make sure it is datatime
            #     row[tag_date] = datetime.datetime.strptime(row[tag_date], '%Y-%m-%dT%H:%M:%SZ').replace(
            #         tzinfo=datetime.timezone.utc)

            tag_int = ['owner', 'race_id', 'station_id', 'system_id', 'type_id', 'location_id', 'owner_id',
                       'solar_system_id']
            for it in tag_int:
                if isinstance(row[it], int) is False:
                    if row[it] > 0:
                        row[it] = int(row[it])
                    else:
                        row[it] = int(0)

            tag_jason = 'services'
            if isinstance(row[tag_jason], list):
                row[tag_jason] = json.dumps(row[tag_jason])
            else:
                row[tag_jason] = json.dumps('')

            # tag_float = ['time_efficiency']
            # if isinstance(row[tag_float], float) is False:
            #     row[tag_float] = float(row[tag_float])

            # tag_bool = 'is_blueprint_copy'
            # if isinstance(row[tag_bool], bool) is False:
            #     if row[tag_bool] > 0:
            #         row[tag_bool] = True
            #     else:
            #         row[tag_bool] = False

            row['position_x'] = row['position.x']
            row.pop('position.x', None)

            row['position_y'] = row['position.y']
            row.pop('position.y', None)

            row['position_z'] = row['position.z']
            row.pop('position.z', None)

            row['last_update'] = now
            # print('')
            # for item in row:
            #     print(row[item])

            tag_unique = 'location_id'

            sql = '''INSERT INTO %s (%s) 
                                   VALUES ( %%(%s)s ) 
                                   ON CONFLICT (%s) 
                                   DO UPDATE
                                   SET 
                                   last_update=EXCLUDED.last_update,
                                   max_dockable_ship_volume=EXCLUDED.max_dockable_ship_volume,
                                   name=EXCLUDED.name,
                                   office_rental_cost=EXCLUDED.office_rental_cost,
                                   owner=EXCLUDED.owner,
                                   race_id=EXCLUDED.race_id,
                                   reprocessing_efficiency=EXCLUDED.reprocessing_efficiency,
                                   reprocessing_stations_take=EXCLUDED.reprocessing_stations_take,
                                   services=EXCLUDED.services,
                                   owner_id=EXCLUDED.owner_id
                                       ''' % (tablename, ',  '.join(row), ')s, %('.join(row), tag_unique)
            # print(sql)
            c.execute(sql, row)

        conn.commit()
        c.execute('select count(*) from %s' % (tablename))
        after = c.fetchone()[0]

        print(colored('{} new records in {}'.format(after - before, tablename), 'green'))
        stamp3 = datetime.datetime.now(datetime.timezone.utc)
        print('ESI done')
        countdown(stamp3, stamp2)
    else:
        print('\nno new location')


# %% connect DB
stamp1 = datetime.datetime.now(datetime.timezone.utc)


def lighter():
    db = 'neweden'
    conn = psycopg2.connect(database=db, user="postgres")
    c = conn.cursor()
    return conn, c


conn, c = lighter()

# %% check now
c.execute('SELECT time from sys_log order by id DESC limit 1')
last = c.fetchone()[0]
# last = datetime.datetime.strptime(last, '%Y-%m-%d %H:%M:%S.%f')
print('Last update in {}'.format(last))
now = datetime.datetime.now(datetime.timezone.utc)
print('Now is {} \n'.format(now))
past = now - last
print('{} days {:.2} hours passed since last update\n'.format(past.days, past.seconds / 60 / 60))
# log now
c.execute('insert into sys_log(time) values(%s)', (now,))
conn.commit()  # update reading date

# %% Prepare ESI

# token
c.execute('SELECT access_token, token_type, expires_in, refresh_token from acc_tokens')
res = c.fetchone()
cols = [desc[0] for desc in c.description]
token = {}
for i in range(len(cols)):
    token[cols[i]] = res[i]

# appkey
c.execute('select redirect_uri, secret_key, client_id from acc_app')
res = c.fetchone()
cols = [desc[0] for desc in c.description]

appkey = {}

for i in range(len(cols)):
    appkey[cols[i]] = res[i]

# appname
c.execute('select appname from acc_app')
res = c.fetchone()
appname = res[0]  # please change to your own appname

# %% cyno
app, security, client = cynoup(app_key=appkey, appname=appname)
# refresh token
tokens, security = refresh_tokens(token, security)
c.execute('''UPDATE acc_tokens
SET access_token = %s, 
expires_in = %s,
 token_type = %s,
 refresh_token = %s,
 refresh_date = %s
 ''', (
    tokens['access_token'], tokens['expires_in'], tokens['token_type'], tokens['refresh_token'],
    datetime.datetime.now(datetime.timezone.utc)))
conn.commit()
stamp2 = datetime.datetime.now(datetime.timezone.utc)
countdown(stamp2, stamp1)
# %% get all type_id
get_type_id = True
if get_type_id is True:
    print(colored('\nget all type_id','green'))
    stamp1 = datetime.datetime.now(datetime.timezone.utc)

    op = app.op['get_universe_types'](page=1)
    res = client.request(op)

    res = client.head(op)

    if res.status == 200:
        number_of_page = res.header['X-Pages'][0]

        # now we know how many pages we want, let's prepare all the requests
        operations = []
        for page in range(1, number_of_page):
            operations.append(
                app.op['get_universe_types'](
                    page=page,
                )
            )

        results = client.multi_request(operations)
    list_typeid = list()
    for page in results:
        for t_id in page[1].data:
            list_typeid.append(t_id)

    # get the list from db
    tablename = 'universe_type_ids'
    c.execute('SELECT type_id from %s' % tablename)
    list_typeid_db = c.fetchall()
    old_list = list()
    for it in list_typeid_db:
        old_list.append(it[0])
    diff = list(set(list_typeid) - set(old_list))

    print('found {} new type_id'.format(len(diff)))
    stamp2 = datetime.datetime.now(datetime.timezone.utc)
    countdown(stamp2, stamp1)

    if len(diff) > 0:
        operations = []
        for it in diff:
            operations.append(app.op['get_universe_types_type_id'](type_id=it))
        res_id = client.multi_request(operations)

        now = datetime.datetime.now(datetime.timezone.utc)

        for it in res_id:
            content = it[1].data

            d = {}
            d['description'] = content.description
            d['group_id'] = content.group_id
            d['name'] = content.name
            d['packaged_volume'] = content.packaged_volume
            d['type_id'] = content.type_id
            d['volume'] = content.volume
            d['metalevel'] = None
            d['techlevel'] = None
            d['metagroup'] = None
            d['last_update'] = now

            try:
                dogma_attributes = content.dogma_attributes
            except:
                dogma_attributes = 0
            ## stop here
            # print(type(dogma_attributes))

            if isinstance(dogma_attributes, int) is False:

                for attr in dogma_attributes:
                    if attr['attribute_id'] == 633:
                        meta = attr['value']
                        d['metalevel'] = int(meta)
                    elif attr['attribute_id'] == 422:
                        tech = attr['value']
                        d['techlevel'] = int(tech)
                    elif attr['attribute_id'] == 1692:
                        metag = attr['value']
                        d['metagroup'] = int(metag)
                    else:
                        continue

            tag_unique = 'type_id'

            sql = '''INSERT INTO %s (%s) 
                                       VALUES ( %%(%s)s ) 
                                       ON CONFLICT (%s) 
                                       DO NOTHING                     
                                           ''' % (tablename, ',  '.join(d), ')s, %('.join(d), tag_unique)
            # print(sql)
            c.execute(sql, d)

        conn.commit()

# %% get all regions names
get_geoinfo = False
if get_geoinfo is True:

    print(colored('\nget all regions','green'))
    url = 'https://esi.evetech.net/latest/universe/regions/?datasource=tranquility'
    res = requests.get(url)
    region_ids = json.loads(res.content)
    opname = 'get_universe_regions_region_id'
    operations = []
    for id in region_ids:
        operations.append(app.op[opname](region_id=id))
    res_id = client.multi_request(operations)  # get all reginon_id
    res_regions = res_id

    erros_cons = []
    erros_sys = []
    uni_geo = []
    # region
    for region in res_regions:
        info = region[1].data
        region_id = info['region_id']
        region_name = info['name']
        try:
            region_des = info['description']
        except:
            region_des = ''
        constellations = info['constellations']
        # print('region_id {}'.format(region_id))

        # constellations
        for cons in constellations:
            constellation_id = cons
            opname = 'get_universe_constellations_constellation_id'
            op = app.op[opname](constellation_id=cons)
            for j in range(0, 4):
                while True:
                    try:
                        res = client.request(op)

                    except Exception as e:
                        print('Error:' + str(e) + "\n")
                        print('constellation_id {} get wrong'.format(constellation_id))
                        if constellation_id not in erros_cons:
                            erros_cons.append(constellation_id)
                        # check the error remain
                        e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                        e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                        e_status = res.status

                        # reaction to error
                        if e_remain < 50:
                            print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                            time.sleep(e_reset)
                            print('sleep {}s'.format(e_reset))
                        if e_status == 403:
                            print(res.raw)
                            break
                        continue
                    break

            res_constellation = res

            info = res.data
            constellation_name = info['name']
            constellation_position = json.dumps(info['position'])
            systems = info['systems']

            print('constellation_id {}'.format(constellation_id))

            # systems
            for sys in systems:
                system_id = sys
                opname = 'get_universe_systems_system_id'
                op = app.op[opname](system_id=sys)

                for i in range(0, 5):
                    while True:
                        try:
                            res = client.request(op)
                        except Exception as e:
                            print('Error:' + str(e) + "\n")
                            print('system_id {} get wrong'.format(sys))
                            if system_id not in erros_sys:
                                erros_sys.append(system_id)
                            # check the error remain
                            e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                            e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                            e_status = res.status

                            # reaction to error
                            if e_remain < 50:
                                print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                                time.sleep(e_reset)
                                print('sleep {}s'.format(e_reset))
                            if e_status == 403:
                                print(res.raw)
                                break
                            continue
                        break
                if i == 5:
                    print('failed {}'.format(system_id))
                    continue

                res_system = res

                info = res.data
                system_name = info['name']
                system_position = json.dumps(info['position'])
                try:
                    system_planets = json.dumps(info['planets'])
                except:
                    system_planets = None
                try:
                    security_class = info['security_class']
                except:
                    security_class = None
                try:
                    security_status = info['security_status']
                except:
                    security_status = None
                try:
                    star_id = info['star_id']
                except:
                    star_id = None
                try:
                    stargates = json.dumps(info['stargates'])
                except:
                    stargates = None

                # print('system_id {}'.format(system_id))

                uni_geo.append(
                    [system_id, constellation_id, region_id, system_name, system_position, system_planets,
                     security_class,
                     security_status, star_id, stargates, constellation_name, constellation_position, region_des,
                     region_name])

    # insert into DB
    if erros_cons == [] and erros_sys == []:
        db = 'neweden'
        conn = psycopg2.connect(database=db, user="postgres")
        c = conn.cursor()

        sql = '''insert into universe_geo
                (system_id , constellation_id, region_id,
                system_name, system_position, system_planets, security_class, security_status, star_id, stargates,
                constellation_name, constellation_position,
                 region_des, region_name)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
                ON CONFLICT (system_id) 
                DO nothing '''

        for row in uni_geo:
            items = []
            for item in row:
                if item == '':
                    item = None
                    items.append(item)
                else:
                    items.append(item)
            try:
                c.execute(sql, items)
            except Exception as e:
                print('Error:' + str(e) + "\n")
                break
        conn.commit()
    else:
        print('pls check your errors first')

# %% get my transaction record
get_tran_record = True

if get_tran_record is True:
    print(colored('\nget my transaction record', 'green'))
    stamp1 = datetime.datetime.now(datetime.timezone.utc)
    opn = 'get_characters_character_id_wallet_transactions'
    api_info = security.verify()
    char_id = api_info['sub'].split(':')[2]
    try:
        op = app.op[opn](character_id=char_id)
        res = client.request(op)

        # if res.status == 200:
    except Exception as e:
        print(e)
    res = json.loads(res.raw)
    cols = list(res[0].keys())
    tablename = 'tad_wal_tran'
    c.execute('select count(*) from %s' % (tablename))
    before = c.fetchone()[0]

    for row in res:
        if isinstance(row['date'], datetime.date) is False:  # make sure it is datatime
            row['date'] = datetime.datetime.strptime(row['date'], '%Y-%m-%dT%H:%M:%SZ').replace(
                tzinfo=datetime.timezone.utc)
        sql = '''INSERT INTO %s (operator, %s) 
        VALUES (%s ,%%(%s)s ) 
        ON CONFLICT (transaction_id,is_buy) 
        DO nothing 
            ''' % (tablename, ',  '.join(row), char_id, ')s, %('.join(row))
        # print(sql)
        # print(row)
        c.execute(sql, row)
    conn.commit()
    c.execute('select count(*) from %s' % (tablename))
    after = c.fetchone()[0]
    stamp2 = datetime.datetime.now(datetime.timezone.utc)

    print(colored('{} new records in {}'.format(after - before, tablename), 'green'))
    countdown(stamp2, stamp1)

    # check location
    location_list = []
    for row in res:
        if row['location_id'] not in location_list:
            location_list.append(row['location_id'])

    add_location(location_list)

# %% get regional public orders
get_reg_orders = True
if get_reg_orders is True:
    print(colored('\nget regional public orders','green'))

    # check time
    c.execute('SELECT max(create_date) as create_date FROM tad_reg_pub_orders')
    latest = c.fetchone()[0]
    now = datetime.datetime.now(datetime.timezone.utc)
    delta = now - latest
    print('since last update')
    countdown(now, latest)
    if delta.total_seconds() / 60 > 5:
        target_reg = [
            10000002,  # the forge
            10000005,  # Detorid
            10000006,  # Wicked Creek
            10000008,  # Scalding Pass
            10000009,  # Insmother
            10000012,  # Curse
            10000025,  # Immensea
            10000061  # Tenerifis
        ]
        stamp1 = datetime.datetime.now(datetime.timezone.utc)
        stamp0 = stamp1

        try:
            del dfs
        except: pass

        for item in target_reg:
            print(item)
            op = app.op['get_markets_region_id_orders'](region_id=item)
            for i in range(0, 5):
                while True:
                    try:
                        res = client.request(op)

                        if res.status != 200:
                            e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                            e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                            e_status = res.status

                            print(e_status, item)

                            # reaction to error
                            if e_remain < 50:
                                print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                                time.sleep(e_reset)
                                print('sleep {}s'.format(e_reset))
                            continue
                    except Exception as e:
                        print('Error:' + str(e) + "\n")

                        # check the error remain
                        e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                        e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                        e_status = res.status

                        # reaction to error
                        if e_remain < 50:
                            print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                            time.sleep(e_reset)
                            print('sleep {}s'.format(e_reset))
                        if e_status == 403:
                            print(res.raw)
                            break
                        continue
                    break

            # get all pages

            if res.status == 200:
                number_of_page = res.header['X-Pages'][0]

                url_cakes=[]
                for page in range(1, number_of_page + 1):
                    url='https://esi.evetech.net/latest/markets/{}/orders/?datasource=tranquility&order_type=all&page={}'.format(str(item),str(page))
                    url_cakes.append(url)

                def get_pub_order(url):

                    for i in range(0, 2):
                        while True:
                            try:
                                res = requests.get(url)
                                if res.status_code == 200:
                                    break
                                else:
                                    e_remain = int(res.headers.get("x-esi-error-limit-remain"))
                                    e_reset = int(res.headers.get("x-esi-error-limit-reset"))
                                    if e_remain < 50:
                                        print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                                        time.sleep(e_reset)
                                        print('sleep {}s'.format(e_reset))
                                    continue
                            except Exception as e:
                                print('Error:' + str(e) + "\n")
                                e_remain = int(res.headers.get("x-esi-error-limit-remain"))
                                e_reset = int(res.headers.get("x-esi-error-limit-reset"))
                                if e_remain < 50:
                                    print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                                    time.sleep(e_reset)
                                    print('sleep {}s'.format(e_reset))
                                continue
                            break

                    if res.status_code == 200:
                        df = pd.io.json.json_normalize(json.loads(res.content))
                        return df
                        # try:
                        #     dfs = dfs.append(df, ignore_index=True, sort=False)
                        # except:
                        #     dfs = df
                    else:
                        print(colored('{} failed to get {}'.format(res.status_code,url),'green'))
                        return {}

                with ThreadPoolExecutor(max_workers=None) as executor:
                    stampin = datetime.datetime.now(datetime.timezone.utc)

                    # executor.map(get_pub_order, pages)

                    futures = [executor.submit(get_pub_order, url) for url in url_cakes]
                    try:
                        del df
                    except: pass

                    for result in as_completed(futures):
                        if result._state=='FINISHED':
                            try:
                                df=df.append(result._result, ignore_index=True, sort=False)
                            except NameError:
                                df = result._result

                    stampout = datetime.datetime.now(datetime.timezone.utc)
                    countdown(stampout,stampin)
                ### Test END
            try:
                dfs = dfs.append(df, ignore_index=True, sort=False)
            except NameError:
                dfs = df

            # stamp2 = datetime.datetime.now(datetime.timezone.utc)
            # countdown(stamp2, stamp0)


        rr0, cc0, = dfs.shape

        dfs = dfs.drop_duplicates(subset=['order_id'])

        rr1, cc1, = dfs.shape
        print('add {} col, removed {} rows of duplicates'.format(cc0 - cc1, rr0 - rr1))

        # insert into postgres
        db = 'neweden'
        conn = psycopg2.connect(database=db, user="postgres")
        c = conn.cursor()
        stamp3 = datetime.datetime.now(datetime.timezone.utc)

        tablename = 'tad_reg_pub_orders'
        c.execute('select count(*) from %s' % (tablename))
        before = c.fetchone()[0]
        cols = list(dfs.columns.values)
        dict = dfs.to_dict(orient='records')
        now = datetime.datetime.now(datetime.timezone.utc)
        stamp4 = datetime.datetime.now(datetime.timezone.utc)
        print('pd used')
        countdown(stamp4, stamp3)

        for row in dict:
            if isinstance(row['issued'], datetime.date) is False:  # make sure it is datatime
                row['issued'] = datetime.datetime.strptime(str(row['issued']), '%Y-%m-%dT%H:%M:%SZ').replace(
                    tzinfo=datetime.timezone.utc)

            sql = '''INSERT INTO %s (operator, create_date, %s) 
                    VALUES (%s, %%(create_date)s ,%%(%s)s ) 
                    ON CONFLICT (order_id) 
                    DO UPDATE
                    SET 
                    update_date = EXCLUDED.create_date,
                    volume_remain=EXCLUDED.volume_remain
                        ''' % (tablename, ',  '.join(row), char_id, ')s, %('.join(row))
            # print(sql)
            row['create_date'] = now
            c.execute(sql, row)
        conn.commit()
        c.execute('select count(*) from %s' % (tablename))
        after = c.fetchone()[0]

        print(colored('{} new records in {}'.format(after - before, tablename), 'green'))

        stamp5 = datetime.datetime.now(datetime.timezone.utc)
        print('insert used:')
        countdown(stamp5, stamp4)

        print('total used')
        countdown(stamp5, stamp1)
    else:
        print('dont too hurry :)')
# %% get alliance and corplist
get_coop = True
if get_coop is True:
    print('\nget alliance and corplist')

    c.execute('SELECT max(last_update) as create_date FROM co_cooplist')
    latest = c.fetchone()[0]
    now = datetime.datetime.now(datetime.timezone.utc)
    delta = now - latest
    print('since last update')
    countdown(now, latest)
    if delta.total_seconds() / 60 > 60 * 24 * 3:
        stamp1 = datetime.datetime.now(datetime.timezone.utc)
        winter_alliances = {'99003581': 'Fraternity.',
                            '99006828': 'The Therapists',
                            '99007498': 'STARCHASER Alliance'
                            }
        # Shake hand with ESI #1 List all current member corporations of an alliance

        op_name = 'get_alliances_alliance_id_corporations'

        try:
            del dfs
        except:
            pass
        # print('Ready to write the data')

        for alliance_id, alliance_name in winter_alliances.items():
            # print(alliance_id, alliance_name)

            op = app.op[op_name](alliance_id=int(alliance_id))

            for i in range(0, 5):
                while True:
                    try:
                        res = client.request(op)
                    except Exception as e:
                        print('Error:' + str(e) + "\n")

                        # check the error remain
                        e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                        e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                        e_status = res.status

                        # reaction to error
                        if e_remain < 50:
                            print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                            time.sleep(e_reset)
                            print('sleep {}s'.format(e_reset))
                        if e_status == 403:
                            print(res.raw)
                            break
                        continue
                    break

            if res.status == 200:
                df = pd.read_json(res.raw)
                df['alliance_name'] = alliance_name
                df['alliance_id'] = int(alliance_id)
            # put all data together

            try:
                dfs
            except NameError:
                dfs = df
            else:
                dfs = dfs.append(df, ignore_index=True, sort=False)

        df_all_corps = dfs
        df_all_corps = df_all_corps.rename(columns={0: "corporation_id"})
        del dfs
        # get the corp inform

        op_name = 'get_corporations_corporation_id'

        for index, row in df_all_corps.iterrows():

            op = app.op[op_name](corporation_id=row['corporation_id'])

            for i in range(0, 5):
                while True:
                    try:
                        res = client.request(op)
                    except Exception as e:
                        print('Error:' + str(e) + "\n")

                        # check the error remain
                        e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                        e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                        e_status = res.status

                        # reaction to error
                        if e_remain < 50:
                            print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                            time.sleep(e_reset)
                            print('sleep {}s'.format(e_reset))
                        if e_status == 403:
                            print(res.raw)
                            break
                        continue
                    break

            # if 200 let's suck the data

            if res.status == 200:
                df = pd.io.json.json_normalize(json.loads(res.raw))
                df['corporation_id'] = row['corporation_id']

            try:
                dfs
            except NameError:
                dfs = df
            else:
                dfs = dfs.append(df, ignore_index=True, sort=False)

        df_all_corps = pd.merge(df_all_corps, dfs, on=['corporation_id', 'alliance_id'])

        # insert to postgres
        conn, c = lighter()
        cols = list(df_all_corps.columns.values)
        dict = df_all_corps.to_dict(orient='records')

        tablename = 'co_cooplist'
        c.execute('select count(*) from %s' % (tablename))
        before = c.fetchone()[0]
        now = datetime.datetime.now(datetime.timezone.utc)

        for row in dict:
            if isinstance(row['date_founded'], datetime.date) is False:  # make sure it is datatime
                row['date_founded'] = datetime.datetime.strptime(row['date_founded'], '%Y-%m-%dT%H:%M:%SZ').replace(
                    tzinfo=datetime.timezone.utc)
            row['last_update'] = now

            sql = '''INSERT INTO %s (%s) 
                    VALUES ( %%(%s)s ) 
                    ON CONFLICT (corporation_id) 
                    DO UPDATE
                    SET 
                    last_update = EXCLUDED.last_update,
                    ceo_id=EXCLUDED.ceo_id,
                    description=EXCLUDED.description,
                    home_station_id=EXCLUDED.home_station_id,
                    member_count=EXCLUDED.member_count,
                    tax_rate=EXCLUDED.tax_rate,
                    ticker=EXCLUDED.ticker,
                    url=EXCLUDED.url
                        ''' % (tablename, ',  '.join(row), ')s, %('.join(row))

            c.execute(sql, row)
        conn.commit()
        c.execute('select count(*) from %s' % (tablename))
        after = c.fetchone()[0]

        print(colored('{} new records in {}'.format(after - before, tablename), 'green'))

        new = list(df_all_corps['corporation_id'])

        sql = 'SELECT corporation_id FROM co_cooplist'
        c.execute(sql)
        old = c.fetchall()
        old_list = []
        for it in old:
            old_list.append(it[0])

        if set(new) == set(old_list) is False:
            print('Need check the corp history')
        stamp4 = datetime.datetime.now(datetime.timezone.utc)
        print('insert used:')
        countdown(stamp4, stamp1)
        del df, df_all_corps, dfs
    else:
        print('dont too hurry :)')

# %% get regional public contract

get_regional_pub_contracts = True
if get_regional_pub_contracts is True:
    print(colored('\nget regional public contract','green'))

    c.execute('SELECT max(last_update) as create_date FROM tad_reg_pub_contracts')
    latest = c.fetchone()[0]
    now = datetime.datetime.now(datetime.timezone.utc)
    delta = now - latest
    print('since last update')
    countdown(now, latest)
    if delta.total_seconds() / 60 > 5:

        stamp1 = datetime.datetime.now(datetime.timezone.utc)
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
            # print('cleaned')
            pass

        for item in region_ids:
            print(item)
            op = app.op[op_name](region_id=item)

            for i in range(0, 5):
                while True:
                    try:
                        res = client.request(op)
                    except Exception as e:
                        print('Error:' + str(e) + "\n")

                        # check the error remain
                        e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                        e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                        e_status = res.status

                        # reaction to error
                        if e_remain < 50:
                            print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                            time.sleep(e_reset)
                            print('sleep {}s'.format(e_reset))
                        if e_status == 403:
                            print(res.raw)
                            break
                        continue
                    break

            df = pd.read_json(res.raw)

            # get all pages

            if res.status == 200:
                number_of_page = res.header['X-Pages'][0]
                if number_of_page > 1:

                    for page in range(1, number_of_page):
                        op = app.op[op_name](region_id=item, page=page)
                        print(page + 1)

                        for i in range(0, 5):
                            while True:
                                try:
                                    res = client.request(op)
                                except Exception as e:
                                    print('Error:' + str(e) + "\n")

                                    # check the error remain
                                    e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                                    e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                                    e_status = res.status

                                    # reaction to error
                                    if e_remain < 50:
                                        print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                                        time.sleep(e_reset)
                                        print('sleep {}s'.format(e_reset))
                                    if e_status == 403:
                                        print(res.raw)
                                        break
                                    continue
                                break

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

        df_contracts = dfs
        print('PD done')
        stamp2 = datetime.datetime.now(datetime.timezone.utc)
        countdown(stamp2, stamp1)

        # read & write
        dict = df_contracts.to_dict(orient='records')

        conn, c = lighter()

        tablename = 'tad_reg_pub_contracts'
        c.execute('select count(*) from %s' % (tablename))
        before = c.fetchone()[0]
        now = datetime.datetime.now(datetime.timezone.utc)

        for row in dict:
            tag_date = 'date_issued'
            if isinstance(row[tag_date], datetime.date) is False:  # make sure it is datatime
                row[tag_date] = datetime.datetime.strptime(row[tag_date], '%Y-%m-%dT%H:%M:%SZ').replace(
                    tzinfo=datetime.timezone.utc)

            tag_date = 'date_expired'
            if isinstance(row[tag_date], datetime.date) is False:  # make sure it is datatime
                row[tag_date] = datetime.datetime.strptime(row[tag_date], '%Y-%m-%dT%H:%M:%SZ').replace(
                    tzinfo=datetime.timezone.utc)

            tag_bool = 'for_corporation'
            if isinstance(row[tag_bool], bool) is False:
                if row[tag_bool] > 0:
                    row[tag_bool] = True
                else:
                    row[tag_bool] = False

            # tag_float='buyout'
            # if isinstance(row[tag_float], float) is False:
            #     row[tag_float]=float(0)

            row['last_update'] = now

            tag_unique = 'contract_id'

            sql = '''INSERT INTO %s (%s) 
                        VALUES ( %%(%s)s ) 
                        ON CONFLICT (%s) 
                        DO NOTHING 
                            ''' % (tablename, ',  '.join(row), ')s, %('.join(row), tag_unique)
            # print(sql)
            c.execute(sql, row)

        conn.commit()
        c.execute('select count(*) from %s' % (tablename))
        after = c.fetchone()[0]

        print(colored('{} new records in {}'.format(after - before, tablename), 'green'))
        stamp3 = datetime.datetime.now(datetime.timezone.utc)
        print('insert used:')
        countdown(stamp3, stamp2)

        #  extract location

        print(colored('\nget stations and structures','green'))

        c.execute('SELECT max(last_update) as create_date FROM universe_stations_temp')
        latest = c.fetchone()[0]
        now = datetime.datetime.now(datetime.timezone.utc)
        delta = now - latest
        print('since last update')
        countdown(now, latest)
        if delta.total_seconds() / 60 > 60*24*2:

            location_list = df_contracts['start_location_id'].drop_duplicates().tolist()

            add_location(location_list)
        else:
            print('station not likely changed')

        # get contract items

        print(colored('\nget items in contracts','green'))

        tablename = 'tad_contracts_items'
        conn, c = lighter()

        list_contract = df_contracts[df_contracts['type'] == 'item_exchange']['contract_id'].drop_duplicates().tolist()
        tag_unique = 'contract_id'
        sql = 'SELECT DISTINCT %s FROM %s' % (tag_unique, tablename)
        c.execute(sql)
        exlist = [x[0] for x in c.fetchall()]

        new_contract = []
        for x in list_contract:
            if x not in exlist:
                new_contract.append(x)

        if len(new_contract)>0:
            op_name = 'get_contracts_public_items_contract_id'

            for item in new_contract:
                # print(item)
                op = app.op[op_name](contract_id=item)

                for i in range(0, 5):
                    while True:
                        try:
                            res = client.request(op)
                        except Exception as e:
                            print('Error:' + str(e) + "\n")

                            # check the error remain
                            e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                            e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                            e_status = res.status

                            # reaction to error
                            if e_remain < 50:
                                print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                                time.sleep(e_reset)
                                print('sleep {}s'.format(e_reset))
                            if e_status == 403:
                                print(res.raw)
                                break
                            continue
                        break

                # get all pages

                if res.status == 200:
                    df_contract_item = pd.read_json(res.raw)
                    number_of_page = res.header['X-Pages'][0]
                    if number_of_page > 1:

                        for page in range(1, number_of_page):
                            op = app.op[op_name](contract_id=item, page=page)
                            print(page + 1)

                            for i in range(0, 5):
                                while True:
                                    try:
                                        res = client.request(op)
                                    except Exception as e:
                                        print('Error:' + str(e) + "\n")

                                        # check the error remain
                                        e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
                                        e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
                                        e_status = res.status

                                        # reaction to error
                                        if e_remain < 50:
                                            print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                                            time.sleep(e_reset)
                                            print('sleep {}s'.format(e_reset))
                                        if e_status == 403:
                                            print(res.raw)
                                            break
                                        continue
                                    break

                            df1 = pd.read_json(res.raw)
                            df_contract_item = df_contract_item.append(df1, ignore_index=True, sort=False)
                else:
                    print(res.status, res.raw)
                    continue

                try:
                    df_contract_items
                except NameError:
                    df_contract_items = df_contract_item
                    df_contract_items['contract_id'] = item
                else:
                    df_contract_item['contract_id'] = item
                    df_contract_items = df_contract_items.append(df_contract_item, ignore_index=True, sort=False)

            # insert into DB
            df_contract_items = df_contract_items.drop_duplicates()

            dict = df_contract_items.to_dict(orient='records')

            c.execute('select count(*) from %s' % (tablename))
            before = c.fetchone()[0]
            now = datetime.datetime.now(datetime.timezone.utc)

            for row in dict:
                # tag_date = 'date_issued'
                # if isinstance(row[tag_date], datetime.date) is False:  # make sure it is datatime
                #     row[tag_date] = datetime.datetime.strptime(row[tag_date], '%Y-%m-%dT%H:%M:%SZ').replace(
                #         tzinfo=datetime.timezone.utc)

                tag_int = 'item_id'
                if isinstance(row[tag_int], bool) is False:
                    if row[tag_int] > 0:
                        row[tag_int] = int(row[tag_int])
                    else:
                        row[tag_int] = None
                try:
                    tag_bool = 'is_blueprint_copy'
                    if isinstance(row[tag_bool], bool) is False:
                        if row[tag_bool] > 0:
                            row[tag_bool] = True
                        else:
                            row[tag_bool] = False
                except: pass


                try:
                    tag_float = 'material_efficiency'
                    if isinstance(row[tag_float], float) is False:
                        row[tag_float] = float(row[tag_float])
                except: pass

                try:
                    tag_float = 'runs'
                    if isinstance(row[tag_float], float) is False:
                        row[tag_float] = float(row[tag_float])
                except: pass

                try:
                    tag_float = 'time_efficiency'
                    if isinstance(row[tag_float], float) is False:
                        row[tag_float] = float(row[tag_float])

                except: pass

                # try:
                # except: pass
                #
                #
                #
                #




                row['last_update'] = now
                # print('')
                # for item in row:
                #     print(row[item])

                tag_unique = ('contract_id', 'item_id', 'record_id', 'quantity')

                sql = '''INSERT INTO %s (%s) 
                               VALUES ( %%(%s)s ) 
                               ON CONFLICT (%s) 
                               DO NOTHING 
                                   ''' % (tablename, ',  '.join(row), ')s, %('.join(row), ',  '.join(tag_unique))
                # print(sql)
                c.execute(sql, row)

            conn.commit()
            c.execute('select count(*) from %s' % (tablename))
            after = c.fetchone()[0]

            print(colored('{} new records in {}'.format(after - before, tablename), 'green'))
            stamp4 = datetime.datetime.now(datetime.timezone.utc)
            print('insert used:')
            countdown(stamp4, stamp3)
        else:
            print('no new contract')

    else:
        print('dont too hurry :)')

# %% get_markets_region_id_types
get_markets_region_id_types = True
print(colored('\nget_markets_region_id_types', 'green'))

stamp1 = datetime.datetime.now(datetime.timezone.utc)
print('get IDs')
region_ids = [
    # 10000012,  # curse
    10000005,  # Detorid
    # # 10000061,  # Tenerifis
    # # 10000009,  # Insmother
    # # 10000025,  # Immensea
    # # 10000006,  # Wicked Creek
    # # 10000008,  # Scalding Pass
    10000002 # the forge
]
try:
    del df
    del dfs
    del df1
    del df_contracts
    del df_contract_item
    del df_contract_items
except:
    # print('cleaned')
    pass

url_cakes = []

for region in region_ids:

    endpoint = 'get_markets_region_id_types'

    # op = app.op[endpoint](region_id=region)
    #
    # for i in range(0, 2):
    #     while True:
    #         try:
    #             res = client.request(op)
    #         except Exception as e:
    #             print('Error:' + str(e) + "\n")
    #
    #             # check the error remain
    #             e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
    #             e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
    #             e_status = res.status
    #
    #             # reaction to error
    #             if e_remain < 50:
    #                 print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
    #                 time.sleep(e_reset)
    #                 print('sleep {}s'.format(e_reset))
    #             if e_status == 403:
    #                 print(res.raw)
    #             continue
    #         break
    # if res.status == 200:
    #     df = json.loads(res.raw)
    #
    # else:
    #     print(res.status, res.raw)
    #     # check the error remain
    #     e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
    #     e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
    #     e_status = res.status
    #
    #     # reaction to error
    #     if e_remain < 50:
    #         print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
    #         time.sleep(e_reset)
    #         print('sleep {}s'.format(e_reset))
    #     continue

    op = app.op[endpoint](region_id=region, page=1)
    res = client.request(op)

    if res.status == 200:
        number_of_page = res.header['X-Pages'][0]

        # now we know how many pages we want, let's prepare all the requests
        operations = []
        for page in range(1, number_of_page):
            operations.append(
                app.op[endpoint](region_id=region,
                                 page=page
                                 )
            )

        results = client.multi_request(operations)

    list_typeid = []

    for page in results:
        for t_id in page[1].data:
            if t_id not in list_typeid:
                list_typeid.append(t_id)

    for id in list_typeid:
        # opname = 'get_markets_region_id_history'

        # op = app.op[endpoint](region_id=region)
        #
        # res = client.request(op)

        url = 'https://esi.evetech.net/latest/markets/{}/history/?datasource=tranquility&type_id={}'.format(str(region),
                                                                                                            str(id))

        # for i in range(0, 2):
        #     while True:
        #         try:
        #             res = requests.get(url, timeout=0.7)
        #         except Exception as e:
        #             print('Error:' + str(e) + "\n")
        #
        #             # check the error remain
        #             e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
        #             e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
        #             e_status = res.status_code
        #
        #             # reaction to error
        #             if e_remain < 50:
        #                 print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
        #                 time.sleep(e_reset)
        #                 print('sleep {}s'.format(e_reset))
        #             if e_status == 403:
        #                 print(res.raw)
        #             continue
        #         break
        #
        # if res.status_code == 200:
        #     region_ids = json.loads(res.content)
        #     df = pd.io.json.json_normalize(json.loads(res.content))
        #     df['type_id'] = id
        #     df['region_id'] = region
        #
        #     try:
        #         dfs = dfs.append(df, ignore_index=True, sort=False)
        #     except:
        #         dfs = df
        # else:
        #     print(res.status_code)

        cake = {}
        cake['url'] = url
        cake['region'] = region
        cake['type_id'] = id
        url_cakes.append(cake)

stamp2 = datetime.datetime.now(datetime.timezone.utc)

countdown(stamp2,stamp1)

def cookcakes(cake):
    url = cake['url']
    region = cake['region']
    id = cake['type_id']
    # esi=requests.Session()

    for i in range(0, 5):

        try:
            res = ESI_SESSION.get(url)

            # if res.status_code != 420:
            #     print(res.status_code,'try+ ', i+1, '\nsleeping')
            #     time.sleep(5)
            #     continue


            if res.status_code != 200:
                # print(res.status_code,'try+ ', i+1, '\nsleeping')
                time.sleep(3)
                e_remain = int(res.headers.get("x-esi-error-limit-remain"))
                e_reset = int(res.headers.get("x-esi-error-limit-reset"))
                if e_remain < 50:
                    print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                    print('sleep {}s'.format(e_reset))
                    time.sleep(e_reset)

                continue
        except Exception as e:
            print('Error:' + str(e) + '\nsleeping')
            time.sleep(3)

            e_remain = int(res.headers.get("x-esi-error-limit-remain"))
            e_reset = int(res.headers.get("x-esi-error-limit-reset"))
            if e_remain < 50:
                print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                print('sleep {}s'.format(e_reset))
                time.sleep(e_reset)

            if res.status_code > 400:
                print(res.status_code)
            continue
        break

    if res.status_code == 200:
        df = pd.io.json.json_normalize(json.loads(res.content))
        df['type_id'] = id
        df['region_id'] = region
        cooked = df.to_dict(orient='records')

        return cooked

        # try:
        #     dfs = dfs.append(df, ignore_index=True, sort=False)
        # except:
        #     dfs = df
    else:
        print(res.status_code,colored('lost', 'red'))

        return {}

print('shake handing')

with ThreadPoolExecutor(max_workers=None) as executor:
    stamp3 = datetime.datetime.now(datetime.timezone.utc)
    ESI_SESSION = requests.Session()
    futures = [executor.submit(cookcakes, cake) for cake in url_cakes]
    results = []
    for result in as_completed(futures):
        results.append(result)

    stamp4 = datetime.datetime.now(datetime.timezone.utc)
    countdown(stamp4, stamp3)

    ### Create a pool of processes. By default, one is created for each CPU in your machine.
    # with ProcessPoolExecutor() as executor:
    #  ### Get a list of files to process
    #  region_ids = [
    #      # 10000012,  # curse
    #      10000005,  # Detorid
    #      # 10000061,  # Tenerifis
    #      # 10000009,  # Insmother
    #      # 10000025,  # Immensea
    #      # 10000006,  # Wicked Creek
    #      # 10000008,  # Scalding Pass
    #  ]
    #  try:
    #      del df
    #      del dfs
    #      del df1
    #      del df_contracts
    #      del df_contract_item
    #      del df_contract_items
    #  except:
    #      # print('cleaned')
    #      pass
    #
    #  url_cakes = []
    #
    #  for region in region_ids:
    #
    #      endpoint = 'get_markets_region_id_types'
    #
    #      # op = app.op[endpoint](region_id=region)
    #      #
    #      # for i in range(0, 2):
    #      #     while True:
    #      #         try:
    #      #             res = client.request(op)
    #      #         except Exception as e:
    #      #             print('Error:' + str(e) + "\n")
    #      #
    #      #             # check the error remain
    #      #             e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
    #      #             e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
    #      #             e_status = res.status
    #      #
    #      #             # reaction to error
    #      #             if e_remain < 50:
    #      #                 print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
    #      #                 time.sleep(e_reset)
    #      #                 print('sleep {}s'.format(e_reset))
    #      #             if e_status == 403:
    #      #                 print(res.raw)
    #      #             continue
    #      #         break
    #      # if res.status == 200:
    #      #     df = json.loads(res.raw)
    #      #
    #      # else:
    #      #     print(res.status, res.raw)
    #      #     # check the error remain
    #      #     e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
    #      #     e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
    #      #     e_status = res.status
    #      #
    #      #     # reaction to error
    #      #     if e_remain < 50:
    #      #         print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
    #      #         time.sleep(e_reset)
    #      #         print('sleep {}s'.format(e_reset))
    #      #     continue
    #
    #      op = app.op[endpoint](region_id=region, page=1)
    #      res = client.request(op)
    #
    #      if res.status == 200:
    #          number_of_page = res.header['X-Pages'][0]
    #
    #          # now we know how many pages we want, let's prepare all the requests
    #          operations = []
    #          for page in range(1, number_of_page):
    #              operations.append(
    #                  app.op[endpoint](region_id=region,
    #                                   page=page
    #                                   )
    #              )
    #
    #          results = client.multi_request(operations)
    #
    #      list_typeid = []
    #
    #      for page in results:
    #          for t_id in page[1].data:
    #              if t_id not in list_typeid:
    #                  list_typeid.append(t_id)
    #
    #      for id in list_typeid:
    #          # opname = 'get_markets_region_id_history'
    #
    #          # op = app.op[endpoint](region_id=region)
    #          #
    #          # res = client.request(op)
    #
    #          url = 'https://esi.evetech.net/latest/markets/{}/history/?datasource=tranquility&type_id={}'.format(
    #              str(region),
    #              str(id))
    #          #
    #          # for i in range(0, 2):
    #          #     while True:
    #          #         try:
    #          #             res = requests.get(url, timeout=0.7)
    #          #         except Exception as e:
    #          #             print('Error:' + str(e) + "\n")
    #          #
    #          #             # check the error remain
    #          #             e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
    #          #             e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
    #          #             e_status = res.status_code
    #          #
    #          #             # reaction to error
    #          #             if e_remain < 50:
    #          #                 print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
    #          #                 time.sleep(e_reset)
    #          #                 print('sleep {}s'.format(e_reset))
    #          #             if e_status == 403:
    #          #                 print(res.raw)
    #          #             continue
    #          #         break
    #          #
    #          # if res.status_code == 200:
    #          #     region_ids = json.loads(res.content)
    #          #     df = pd.io.json.json_normalize(json.loads(res.content))
    #          #     df['type_id'] = id
    #          #     df['region_id'] = region
    #          #
    #          #     try:
    #          #         dfs = dfs.append(df, ignore_index=True, sort=False)
    #          #     except:
    #          #         dfs = df
    #          # else:
    #          #     print(res.status_code)
    #          cake = {}
    #          cake['url'] = url
    #          cake['region'] = region
    #          cake['type_id'] = id
    #          url_cakes.append(cake)
    #
    #  ### Process the list of files, but split the work across the process pool to use all CPUs
    #  ### Loop through all jpg files in the current folder
    #  ### Resize each one to size 600x600
    #  executor.map(cookcakes, url_cakes)


# session = FuturesSession(executor=ThreadPoolExecutor(max_workers=4))
#
# def bg_cb(resp,type_id,region):
#     # parse the json storing the result on the response object
#     resp.data = resp.json()
#     resp.typeid= type_id
#     resp.region=region
#
# with FuturesSession(executor=ThreadPoolExecutor(max_workers=None)) as session:
#     stamp2 = datetime.datetime.now(datetime.timezone.utc)
#     results=[]
#     futures= [session.get(cake['url'], background=bg_cb(type_id=cake['type_id'],region=cake['region'])) for cake in url_cakes]
#
#     results=[]
#     for result in as_completed(futures):
#         # if result.result().status_code ==200:
#         #     # if len(result.result().content)> 2:
#         #     #     df = pd.io.json.json_normalize(json.loads(result.result().content))
#         #     #     try:
#         #     #         dfs = dfs.append(df, ignore_index=True, sort=False)
#         #     #     except:
#         #     #         dfs = df
#         #
#         #     results.append(result.result().content)
#         # else:
#         #     print(result.result().status_code)
#         results.append(result)
#     stamp3 = datetime.datetime.now(datetime.timezone.utc)
#     countdown(stamp3, stamp2)





### insert into db
print('fuck the database')
tablename = 'tad_reg_order_history'
now = datetime.datetime.now(datetime.timezone.utc)

# dict = dfs.drop_duplicates().to_dict(orient='records')
stamp5 = datetime.datetime.now(datetime.timezone.utc)
conn, c = lighter()

c.execute('select count(*) from %s' % (tablename))
before = c.fetchone()[0]

# def cookdb(results):
for result in results:
    dict = result._result
    if dict is not None:
        if len(dict) > 0:
            for row in dict:
                # print(row)
                # tag_date = 'date_issued'
                # if isinstance(row[tag_date], datetime.date) is False:  # make sure it is datatime
                #     row[tag_date] = datetime.datetime.strptime(row[tag_date], '%Y-%m-%dT%H:%M:%SZ').replace(
                #         tzinfo=datetime.timezone.utc)

                tag_date = 'date'
                if isinstance(row[tag_date], datetime.date) is False:  # make sure it is datatime
                    row[tag_date] = datetime.datetime.strptime(row[tag_date], '%Y-%m-%d')

                tag_int = ['order_count', 'volume']
                for tag in tag_int:
                    if isinstance(row[tag], bool) is False:
                        if row[tag] > 0:
                            row[tag] = int(row[tag])
                        else:
                            row[tag] = None
                #
                # tag_bool = 'is_blueprint_copy'
                # if isinstance(row[tag_bool], bool) is False:
                #     if row[tag_bool] > 0:
                #         row[tag_bool] = True
                #     else:
                #         row[tag_bool] = False
                #
                # tag_float = 'material_efficiency'
                # if isinstance(row[tag_float], float) is False:
                #     row[tag_float] = float(row[tag_float])

                row['last_update'] = now
                # print('')
                # for item in row:
                #     print(row[item])

                tag_unique = ('type_id', 'region_id', 'date')

                sql = '''INSERT INTO %s (%s) 
                               VALUES ( %%(%s)s ) 
                               ON CONFLICT (%s) 
                               DO NOTHING 
                                   ''' % (tablename, ',  '.join(row), ')s, %('.join(row), ',  '.join(tag_unique))
                # print(sql)
                c.execute(sql, row)

    # with ThreadPoolExecutor(max_workers=None) as executor:
    #     stamp4 = datetime.datetime.now(datetime.timezone.utc)
    #
    #
    #     executor.map(cookdb, results)
    #
    #     # futures = [executor.submit(cookcakes, cake) for cake in url_cakes]
    #     # results = []
    #     # for result in as_completed(futures):
    #     #     results.append(result)
    #
    #     stamp5 = datetime.datetime.now(datetime.timezone.utc)
    #     countdown(stamp3, stamp2)

conn.commit()
c.execute('select count(*) from %s' % (tablename))
after = c.fetchone()[0]
print(colored('{} new records in {}'.format(after - before, tablename), 'green'))
stamp6 = datetime.datetime.now(datetime.timezone.utc)
countdown(stamp6, stamp5)
