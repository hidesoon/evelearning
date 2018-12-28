import datetime
import io
import json
import re
import time
from concurrent.futures import as_completed, ThreadPoolExecutor

import pandas as pd
import psycopg2
import requests
from esipy import App
from esipy import EsiClient
from esipy import EsiSecurity
from requests_futures.sessions import FuturesSession
from sqlalchemy import create_engine
from sqlalchemy import types as tp
from termcolor import colored


# import grequests as grequests


def cynoup(app_key, appname):
    # app = App.create(url="https://esi.tech.ccp.is/latest/swagger.json?datasource=tranquility")
    app = App.create(url="https://esi.evetech.net/latest/swagger.json?datasource=tranquility")

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
    stamp1 = datetime.datetime.now(datetime.timezone.utc)

    def_loco = pd.read_sql('universe_stations_temp', con=engine)

    def_loco.drop_duplicates(subset='location_id')

    before = len(def_loco.index)
    # print('\nfound {} new locations'.format(len(location_new)))
    # chop the coming task
    if before > 0:
        def_loco = def_loco[~def_loco['location_id'].isin(location_list)]
    # update the coming task
    # anc = 0
    try:
        del def_loc
    except:
        pass
    for location_id in location_list:
        # anc+=1
        # if anc>20:
        #     break

        if location_id < 1000000000000:
            endpoint = 'get_universe_stations_station_id'

            op = app.op[endpoint](station_id=location_id)
            tag_station = 'station'

        else:
            endpoint = 'get_universe_structures_structure_id'

            op = app.op[endpoint](structure_id=location_id)

            tag_station = 'structure'

        for i in range(0, 2):
            while True:
                try:
                    res = client.request(op)
                except Exception as e:
                    print('Error:' + str(e) + "\n")
                    print('Come here1')

                    continue
                break
        if res.status == 200:
            df = pd.io.json.json_normalize(json.loads(res.raw))
            df['pos_type'] = tag_station
            df['location_id'] = location_id
            df['ACL'] = True
        else:
            if res.status == 403:
                df = {}
                df['location_id'] = location_id
                df['pos_type'] = tag_station
                df['ACL'] = False
                df = pd.DataFrame.from_dict([df])
                print('ACL updated')
            else:
                print('OMG')

            # check the error remain
            e_remain = int(res.header.get("x-esi-error-limit-remain")[0])
            e_reset = int(res.header.get("x-esi-error-limit-reset")[0])
            e_status = res.status
            # reaction to error
            if e_remain < 50:
                print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
                time.sleep(e_reset)
                print('sleep {}s'.format(e_reset))

        try:
            def_loc
        except NameError:
            def_loc = df
        try:
            def_loc = def_loc.append(df, ignore_index=True, sort=False)
        except Exception as e:
            print('Error:' + str(e) + "\n")
            print('Come here3')

    stamp2 = datetime.datetime.now(datetime.timezone.utc)
    print('ESI done')
    countdown(stamp2, stamp1)

    #  read & write

    now = datetime.datetime.now(datetime.timezone.utc)

    def_loc['lastupdate'] = now

    if before > 0:
        def_loc = def_loc.append(def_loco, ignore_index=True, sort=False)

    def_loc = def_loc.drop_duplicates(subset='location_id')

    # def_loc.drop_duplicates()
    # print('hehe')

    def_loc.to_sql('universe_stations_temp', con=engine, index=False, if_exists='replace',
                   dtype={'lastupdate': tp.TIMESTAMP(timezone=True)
                          })

    # print('baba')

    after = len(def_loc.index)

    print(colored('{} new records in universe_stations_temp'.format(after - before), 'green'))
    stamp3 = datetime.datetime.now(datetime.timezone.utc)
    countdown(stamp3, stamp2)


# %% connect DB
stamp00 = datetime.datetime.now(datetime.timezone.utc)
stamp1 = datetime.datetime.now(datetime.timezone.utc)


def lighter():
    db = 'neweden'
    conn = psycopg2.connect(database=db, user="postgres")
    c = conn.cursor()
    return conn, c


conn, c = lighter()

# dont forget to import event


engine = create_engine('postgresql://postgres@localhost:5432/neweden')


def df2csv2sql(df_input, tablename):
    stampA = datetime.datetime.now(datetime.timezone.utc)
    # df_input.head(0).to_sql(tablename, con=engine, if_exists='replace', index=False)  # truncates the table

    engine.execute('TRUNCATE ONLY %s' % tablename)
    print('table replaced')

    conne = engine.raw_connection()
    cur = conne.cursor()

    try:
        del output
    except:
        pass
    output = io.StringIO()
    df_input.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    # contents = output.getvalue()
    cur.copy_from(output, tablename, null="")  # null values become ''
    conne.commit()
    conne.close()
    stampB = datetime.datetime.now(datetime.timezone.utc)
    print('write finished')
    countdown(stampB, stampA)
    cur.close()


## check now
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

## Prepare ESI

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
del token, tokens
stamp2 = datetime.datetime.now(datetime.timezone.utc)
countdown(stamp2, stamp1)
# %% get all type_id
get_type_id = True
if get_type_id is True:
    print(colored('\nget all type_id', 'green'))
    stamp1 = datetime.datetime.now(datetime.timezone.utc)

    op = app.op['get_universe_types'](page=1)
    res = client.request(op)

    res = client.head(op)

    if res.status == 200:
        number_of_page = res.header['X-Pages'][0]

        # now we know how many pages we want, let's prepare all the requests
        operations = []
        for page in range(1, number_of_page + 1):
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
    list_old = list()
    for it in list_typeid_db:
        list_old.append(it[0])
    diff = list(set(list_typeid) - set(list_old))

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

        ## group id
        opname = 'get_universe_groups'
        op = app.op[opname](page=1)

        res = client.request(op)

        if res.status == 200:
            number_of_page = res.header['X-Pages'][0]

            # now we know how many pages we want, let's prepare all the requests
            operations = []
            for page in range(1, number_of_page + 1):
                operations.append(
                    app.op[opname](
                        page=page,
                    )
                )

            results = client.multi_request(operations)

        list_ids = list()
        for page in results:
            for t_id in page[1].data:
                list_ids.append(t_id)

        opname = 'get_universe_groups_group_id'
        try:
            del dfs
        except:
            pass

        operations = []
        for id in list_ids:
            op = app.op[opname](group_id=id)
            operations.append(op)
        results = client.multi_request(operations)
        for result in results:
            df = pd.io.json.json_normalize(json.loads(result[1].raw))
            try:
                dfs = dfs.append(df, ignore_index=True, sort=False)
            except:
                dfs = df
        dfs = dfs.drop(columns=['types'])
        dfs = dfs.drop_duplicates()
        dfs.set_index('group_id')
        dfs.to_sql('universe_groupids', con=engine, index=False, if_exists='replace')

        ## categories id
        opname = 'get_universe_categories'
        op = app.op[opname]

        # res = client.request(op)

        res = requests.get('https://esi.evetech.net/latest/universe/categories/?datasource=tranquility')

        list_ids = json.loads(res.content)

        opname = 'get_universe_categories_category_id'
        try:
            del dfs
        except:
            pass

        operations = []
        for id in list_ids:
            op = app.op[opname](category_id=id)
            operations.append(op)
        results = client.multi_request(operations)
        for result in results:
            df = pd.io.json.json_normalize(json.loads(result[1].raw))
            try:
                dfs = dfs.append(df, ignore_index=True, sort=False)
            except:
                dfs = df
        dfs = dfs.drop(columns=['groups'])
        dfs = dfs.drop_duplicates()

        dfs.to_sql('universe_categoryids', con=engine, index=False, if_exists='replace')

        # get all togetger

        df_type = pd.read_sql('universe_type_ids', con=engine)

        # group
        df_groups = pd.read_sql('universe_groupids', con=engine)
        # cat
        df_cat = pd.read_sql('universe_categoryids', con=engine)

        df_type = df_type.drop(columns=['description', 'last_update'])

        df_type = df_type.rename(columns={'name': 'type_name'})

        df_groups = df_groups.rename(columns={'name': 'group_name'})

        df_cat = df_cat.rename(columns={'name': 'cat_name'})

        df = pd.merge(df_type, df_groups, on='group_id', how='left')

        df = pd.merge(df, df_cat, on='category_id', how='left')

        df = df.drop(columns={'published_y', 'published_x'})

        df = df[
            ['type_id', 'type_name', 'group_id', 'group_name', 'category_id', 'cat_name', 'packaged_volume', 'volume',
             'metalevel', 'techlevel', 'metagroup']]

        df.to_sql('universe_ids', con=engine, index=False, if_exists='replace')

        del df, df_cat, df_type, df_groups
        del cols, get_type_id, it, list_old, list_typeid, list_typeid_db, number_of_page, op, operations, page, res, results, t_id, tokens
# %% get all regions names
get_geoinfo = False
if get_geoinfo is True:

    print(colored('\nget all regions', 'green'))
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

# %% get alliance and corplist
get_coop = True
if get_coop is True:

    conn, c = lighter()

    print(colored('\nget alliance and corplist', 'green'))

    c.execute('SELECT max(last_update) as create_date FROM co_cooplist')
    latest = c.fetchone()[0]
    latest = datetime.datetime.strptime(''.join(latest.rsplit(':', 1)), '%Y-%m-%d %H:%M:%S.%f%z')
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
        # cols = list(df_all_corps.columns.values)
        dict = df_all_corps.to_dict(orient='records')

        tablename = 'co_cooplist'
        c.execute('select count(*) from %s' % (tablename))
        before = c.fetchone()[0]
        now = datetime.datetime.now(datetime.timezone.utc)

        for row in dict:
            if isinstance(row['date_founded'], datetime.date) is False:  # make sure it is datatime
                row['date_founded'] = str(datetime.datetime.strptime(row['date_founded'], '%Y-%m-%dT%H:%M:%SZ').replace(
                    tzinfo=datetime.timezone.utc))
            row['last_update'] = str(now)

            # sql = '''INSERT INTO %s (%s)
            #         VALUES ( %%(%s)s )
            #         ON CONFLICT (corporation_id)
            #         DO UPDATE
            #         SET
            #         last_update = EXCLUDED.last_update,
            #         ceo_id=EXCLUDED.ceo_id,
            #         description=EXCLUDED.description,
            #         home_station_id=EXCLUDED.home_station_id,
            #         member_count=EXCLUDED.member_count,
            #         tax_rate=EXCLUDED.tax_rate,
            #         ticker=EXCLUDED.ticker,
            #         url=EXCLUDED.url
            #             ''' % (tablename, ',  '.join(row), ')s, %('.join(row))
            #
            # c.execute(sql, row)
        # conn.commit()

        df = pd.DataFrame(dict)

        stampin = datetime.datetime.now(datetime.timezone.utc)
        conn, c = lighter()
        c.execute('TRUNCATE ONLY %s' % tablename)
        conn.commit()
        print('TRUNCATED')

        try:
            del output
        except:
            pass
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        # contents = output.getvalue()
        c.copy_from(output, tablename, null="")  # null values become ''
        conn.commit()
        conn.close()
        c.close()
        stampout = datetime.datetime.now(datetime.timezone.utc)
        print('insert completed')
        countdown(stampout, stampin)


        c.execute('select count(*) from %s' % (tablename))
        after = c.fetchone()[0]

        print(colored('{} new records in {}'.format(after - before, tablename), 'green'))

        new = list(df_all_corps['corporation_id'])

        sql = 'SELECT corporation_id FROM co_cooplist'
        c.execute(sql)
        old = c.fetchall()
        list_old = []
        for it in old:
            list_old.append(it[0])

        if set(new) == set(list_old) is False:
            print('Need check the corp history')
        stamp4 = datetime.datetime.now(datetime.timezone.utc)
        print('insert used:')
        countdown(stamp4, stamp1)
        del df, df_all_corps, dfs
    else:
        print('dont too hurry :)')

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
    try:
        del res, results
    except:
        pass

    # List open market orders placed by a character
    print(colored('\nget my order record', 'green'))
    stampA = datetime.datetime.now(datetime.timezone.utc)
    opn = 'get_characters_character_id_orders'
    try:
        op = app.op[opn](character_id=char_id)
        res = client.request(op)

        # if res.status == 200:
    except Exception as e:
        print(e)
    df = pd.io.json.json_normalize(json.loads(res.raw))

    # 2 pg

    tablename = 'tad_my_orders'
    now = datetime.datetime.now(datetime.timezone.utc)
    df['lastupdate'] = now

    df.to_sql(tablename, con=engine, index=False, if_exists='replace',
              dtype={'lastupdate': tp.TIMESTAMP(timezone=True)
                     })

    try:
        del before, cols, df, res
    except:
        pass

# %% get regional public contract

get_regional_pub_contracts = True
if get_regional_pub_contracts is True:
    print(colored('\nget regional public contract', 'green'))

    c.execute('SELECT max(last_update) as create_date FROM tad_reg_pub_contracts')
    latest = c.fetchone()[0]
    now = datetime.datetime.now(datetime.timezone.utc)
    delta = now - latest
    print('since last update')
    countdown(now, latest)
    if delta.total_seconds() / 60 > 5:

        stamp1 = datetime.datetime.now(datetime.timezone.utc)
        op_name = 'get_contracts_public_region_id'
        region_ids = [
            10000005,  # Detorid
            10000012,  # curse
            # 10000061,  # Tenerifis
            # 10000009,  # Insmother
            # 10000025,  # Immensea
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
                        DO UPDATE
                        SET
                        last_update = EXCLUDED.last_update
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

        print(colored('\nget stations and structures', 'green'))

        c.execute('SELECT max(lastupdate) as create_date FROM universe_stations_temp')

        def_loco = pd.read_sql('universe_stations_temp', con=engine)

        def_loco.drop_duplicates(subset='location_id')

        before = len(def_loco.index)

        location_list = df_contracts['start_location_id'].drop_duplicates().tolist()

        location_list_old = def_loco['location_id'].tolist()

        new = list(set(location_list) - set(location_list))

        print('found {} new location'.format(len(new)))

        if len(new) > 0:

            add_location(location_list)
        else:
            print('station not likely changed')

        # get contract items

        print(colored('\nget items in contracts', 'green'))

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

        if len(new_contract) > 0:
            print(colored('found {} new pub contract'.format(len(new_contract), 'green')))
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
                except:
                    pass

                try:
                    tag_float = 'material_efficiency'
                    if isinstance(row[tag_float], float) is False:
                        row[tag_float] = float(row[tag_float])
                except:
                    pass

                try:
                    tag_float = 'runs'
                    if isinstance(row[tag_float], float) is False:
                        row[tag_float] = float(row[tag_float])
                except:
                    pass

                try:
                    tag_float = 'time_efficiency'
                    if isinstance(row[tag_float], float) is False:
                        row[tag_float] = float(row[tag_float])

                except:
                    pass

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

try:
    del def_loco, dict, exlist, list_contract, location_list, location_list_old, new, new_contract,
    del df, df_contract_item, df_contract_items, df_contracts, dfs, res
except:
    pass
# %% get public orders
get_reg_orders = True
if get_reg_orders is True:
    print(colored('\nget  public orders', 'green'))

    # check time
    c.execute('SELECT max(lastupdate) as lastupdate FROM tad_orders_temp')
    latest = c.fetchone()[0]
    # latest=latest+':00'
    latest = datetime.datetime.strptime(''.join(latest.rsplit(':', 1)), '%Y-%m-%d %H:%M:%S.%f%z')

    now = datetime.datetime.now(datetime.timezone.utc)
    delta = now - latest
    print('since last update')
    countdown(now, latest)
    if delta.total_seconds() / 60 > 5:
        target_reg = [
            10000002,  # the forge
            10000005,  # Detorid
            # 10000006,  # Wicked Creek
            # 10000008,  # Scalding Pass
            # 10000009,  # Insmother
            # 10000012,  # Curse
            # # 10000025,  # Immensea
            # # 10000061  # Tenerifis
        ]
        stamp1 = datetime.datetime.now(datetime.timezone.utc)
        stamp0 = stamp1
        # 1 get item from all region (pub ord)

        print(colored('\nget  public orders in region', 'green'))

        url_cakes = []
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

                for page in range(1, number_of_page + 1):
                    url = 'https://esi.evetech.net/latest/markets/{}/orders/?datasource=tranquility&order_type=all&page={}'.format(
                        str(item), str(page))
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

                # df = pd.io.json.json_normalize(json.loads(res.content))

                df = json.loads(res.content)
                return df
                # try:
                #     dfs = dfs.append(df, ignore_index=True, sort=False)
                # except:
                #     dfs = df
            else:
                print(colored('{} failed to get {}'.format(res.status_code, url), 'green'))
                return {}


        results_order = []
        stampin = datetime.datetime.now(datetime.timezone.utc)
        with ThreadPoolExecutor(max_workers=None) as executor:

            # executor.map(get_pub_order, pages)

            futures = [executor.submit(get_pub_order, url) for url in url_cakes]

            for result in as_completed(futures):
                if result._state == 'FINISHED':

                    if isinstance(result._result, list):
                        # try:
                        #     df
                        # except:
                        #     df = result._result
                        #     continue
                        #
                        # try:
                        #     df = df.append(result._result, ignore_index=True, sort=False)
                        # except:
                        #     pass
                        for order in result._result:
                            results_order.append(order)

        stampout = datetime.datetime.now(datetime.timezone.utc)
        countdown(stampout, stampin)

        # 2 get item from all structures
        stampA = datetime.datetime.now(datetime.timezone.utc)
        list_ids = pd.read_sql(
            'SELECT DISTINCT location_id FROM universe_stations_temp WHERE universe_stations_temp.solar_system_id>0'.format(
                str('structure')),
            con=engine)

        opname = 'get_markets_structures_structure_id'

        print(colored('\nget  public orders in structure', 'green'))
        tgt = list_ids['location_id'].tolist()  # "QRFJ-Q - WC starcity"
        for structure_id in tgt:
            op = app.op[opname](structure_id=int(structure_id))

            res = client.request(op)

            if res.status == 200:
                number_of_page = res.header['X-Pages'][0]

                # now we know how many pages we want, let's prepare all the requests
                operations = []
                for page in range(1, number_of_page + 1):
                    operations.append(
                        app.op[opname](
                            structure_id=structure_id, page=page
                        )
                    )

                results = client.multi_request(operations)

                for result in results:
                    df = json.loads(result[1].raw)
                    for order in df:
                        results_order.append(order)

        stampB = datetime.datetime.now(datetime.timezone.utc)
        countdown(stampB, stampA)

        # clear
        df_ords = pd.DataFrame(results_order)

        rr0, cc0, = df_ords.shape
        df_ords = df_ords.drop_duplicates(subset=['order_id'], keep='first')
        rr1, cc1, = df_ords.shape
        print('add {} col, removed {} rows of duplicates'.format(cc0 - cc1, rr0 - rr1))

        # insert into postgres

        df_ords['issued'] = pd.to_datetime(df_ords['issued'], utc=True).astype(pd.Timestamp)

        # dfs=dfs.fillna(0).astype({'system_id':'int64'}, errors='ignore')
        df_ords['lastupdate'] = datetime.datetime.now(datetime.timezone.utc)
        df_ords = df_ords.drop(columns='system_id')
        num = len(df_ords.index)
        print('Found {} active orders'.format(num))

        tablename = 'tad_orders_temp'
        df = df_ords

        stampin = datetime.datetime.now(datetime.timezone.utc)
        df_old = pd.read_sql('tad_orders_temp', con=engine)
        list_old = df_old['order_id'].values
        list_new = df['order_id'].values
        stampout = datetime.datetime.now(datetime.timezone.utc)
        countdown(stampout, stampin)

        stampin = datetime.datetime.now(datetime.timezone.utc)
        s = set(list_new)
        list_expried = [x for x in list_old if x not in s]
        list_update = [x for x in list_old if x in s]
        print('Found {} inactive orders'.format(len(list_expried)))
        print('Updated {} exist orders'.format(len(list_update)))
        print('Found {} new orders'.format(num - len(list_update)))
        stampout = datetime.datetime.now(datetime.timezone.utc)
        countdown(stampout, stampin)

        df_old = df_old[df_old['order_id'].isin(list_expried)]

        df_old['is_out'] = True
        df['is_out'] = False
        df = df.append(df_old, ignore_index=True, sort=False)

        # df_ords['issued'] = df_ords['issued'].apply(lambda x: str(datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc)))

        # for idx, item in df_ords['system_id'].tolist():
        #     if isinstance(item,int) is False:
        #         print(idx,item)

        stamp5 = datetime.datetime.now(datetime.timezone.utc)

        print('start to write PG')

        # conne = engine.raw_connection()
        # conne.close()

        # ## method 1 pd.to_sql

        # def method1(df):
        #     stampA = datetime.datetime.now(datetime.timezone.utc)
        #
        #     df.to_sql('tad_orders_temp', con=engine, index=False, if_exists='replace',
        #               dtype={'issued': sqlalchemy.types.TIMESTAMP(timezone=True),
        #                      'lastupdate': sqlalchemy.types.TIMESTAMP(timezone=True)
        #                      })
        #     stampB = datetime.datetime.now(datetime.timezone.utc)
        #     print('write finished')
        #
        #     countdown(stampB, stampA)

        # method1(df_ords)

        ## method 2

        # stampin = datetime.datetime.now(datetime.timezone.utc)
        # list_expried = list(set(list_old) - set(list_new))
        # stampout = datetime.datetime.now(datetime.timezone.utc)
        # countdown(stampout, stampin)

        # stampin = datetime.datetime.now(datetime.timezone.utc)
        # list_expried = [item for item in list_old if item not in list_new]
        # stampout = datetime.datetime.now(datetime.timezone.utc)
        # countdown(stampout, stampin)

        # method 3
        # stampin = datetime.datetime.now(datetime.timezone.utc)
        # conn, c = lighter()
        # c.execute('TRUNCATE ONLY %s' % tablename)
        # conn.commit()
        # print('TRUNCATED')
        #
        # df_columns = list(df)
        #
        # # create (col1,col2,...)
        # columns = ",".join(df_columns)
        #
        # # create VALUES('%s', '%s",...) one '%s' per column
        # values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        #
        # # create INSERT INTO table (columns) VALUES('%s',...)
        # # insert_stmt = "INSERT INTO {} ({}) {} ON CONFLICT (order_id,is_buy_order) DO UPDATE SET lastupdate = EXCLUDED.lastupdate, volume_remain=EXCLUDED.volume_remain".format(tablename, columns, values)
        # insert_stmt = "INSERT INTO {} ({}) {}".format(tablename, columns, values)
        #
        #
        # psycopg2.extras.execute_batch(c, insert_stmt, df.values)
        # conn.commit()
        # c.close()
        # stampout = datetime.datetime.now(datetime.timezone.utc)
        # print('insert completed')
        # countdown(stampout, stampin)

        stampin = datetime.datetime.now(datetime.timezone.utc)
        conn, c = lighter()
        c.execute('TRUNCATE ONLY %s' % tablename)
        conn.commit()
        print('TRUNCATED')

        try:
            del output
        except:
            pass
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        # contents = output.getvalue()
        c.copy_from(output, tablename, null="")  # null values become ''
        conn.commit()
        conn.close()
        c.close()
        stampout = datetime.datetime.now(datetime.timezone.utc)
        print('insert completed')
        countdown(stampout, stampin)

        # df2csv2sql(df, tablename)
        #
        # stampA = datetime.datetime.now(datetime.timezone.utc)
        # df.head(0).to_sql(tablename, con=engine, if_exists='replace', index=False)  # truncates the table
        #
        # print('table replaced')
        #
        # conne = engine.raw_connection()
        # cur = conne.cursor()
        # try:
        #     del output
        # except:
        #     pass
        # output = io.StringIO()
        # df.to_csv(output, sep='\t', header=False, index=False)
        # output.seek(0)
        # # contents = output.getvalue()
        # cur.copy_from(output, tablename, null="")  # null values become ''
        # conne.commit()
        # conne.close()
        # stampB = datetime.datetime.now(datetime.timezone.utc)
        # print('write finished')
        # countdown(stampB, stampA)
        # cur.close()
        # conne.close()

        # stampA = datetime.datetime.now(datetime.timezone.utc)
        # cols = list(df_ords.columns.values)
        #
        # dict=df_ords.to_dict(orient='records')
        # stampB = datetime.datetime.now(datetime.timezone.utc)
        # print('pd used')
        #
        # countdown(stampB,stampA)
        #
        #
        # #
        # #
        # #
        # #
        # #
        # db = 'neweden'
        # conn = psycopg2.connect(database=db, user="postgres")
        # c = conn.cursor()
        #
        # tablename = 'tad_orders_temp'
        # c.execute('select count(*) from %s' % (tablename))
        # before = c.fetchone()[0]
        # now = datetime.datetime.now(datetime.timezone.utc)
        #
        # stampA = datetime.datetime.now(datetime.timezone.utc)
        # df_ords.head(0).to_sql(tablename, con=engine, if_exists='replace', index=False)  # truncates the table
        #
        # stampB = datetime.datetime.now(datetime.timezone.utc)
        #
        # countdown(stampB, stampA)
        #
        #
        # for row in dict:
        #     # if isinstance(row['issued'], datetime.date) is False:  # make sure it is datatime
        #     #     row['issued'] = datetime.datetime.strptime(str(row['issued']), '%Y-%m-%dT%H:%M:%SZ').replace(
        #     #         tzinfo=datetime.timezone.utc)
        #     row['lastupdate']=now
        #
        #     sql = '''INSERT INTO %s (%s)
        #             VALUES (%%(%s)s )
        #             ON CONFLICT (order_id)
        #             DO UPDATE
        #             SET
        #             lastupdate = EXCLUDED.lastupdate,
        #             volume_remain=EXCLUDED.volume_remain
        #                 ''' % (tablename, ',  '.join(row), ')s, %('.join(row))
        #     # print(sql)
        #     c.execute(sql, row)
        # conn.commit()
        # c.execute('select count(*) from %s' % (tablename))
        # after = c.fetchone()[0]
        #
        # print(colored('{} new records in {}'.format(after - before, tablename), 'green'))
        #
        # # del df_ords
        #
        # # db = 'neweden'
        # # conn = psycopg2.connect(database=db, user="postgres")
        # # c = conn.cursor()
        # # stamp3 = datetime.datetime.now(datetime.timezone.utc)
        # #
        # # tablename = 'tad_reg_pub_orders'
        # # c.execute('select count(*) from %s' % (tablename))
        # # before = c.fetchone()[0]
        # # cols = list(dfs.columns.values)
        # # dict = dfs.to_dict(orient='records')
        # # now = datetime.datetime.now(datetime.timezone.utc)
        # # stamp4 = datetime.datetime.now(datetime.timezone.utc)
        # # print('pd used')
        # # countdown(stamp4, stamp3)
        # #
        # # for row in dict:
        # #     if isinstance(row['issued'], datetime.date) is False:  # make sure it is datatime
        # #         row['issued'] = datetime.datetime.strptime(str(row['issued']), '%Y-%m-%dT%H:%M:%SZ').replace(
        # #             tzinfo=datetime.timezone.utc)
        # #
        # #     sql = '''INSERT INTO %s (operator, create_date, %s)
        # #             VALUES (%s, %%(create_date)s ,%%(%s)s )
        # #             ON CONFLICT (order_id)
        # #             DO UPDATE
        # #             SET
        # #             update_date = EXCLUDED.create_date,
        # #             volume_remain=EXCLUDED.volume_remain
        # #                 ''' % (tablename, ',  '.join(row), char_id, ')s, %('.join(row))
        # #     # print(sql)
        # #     row['create_date'] = now
        # #     c.execute(sql, row)
        # # conn.commit()
        # # c.execute('select count(*) from %s' % (tablename))
        # # after = c.fetchone()[0]
        # #
        # # print(colored('{} new records in {}'.format(after - before, tablename), 'green'))

        try:
            del df, df_columns, df_old, df_ords, executor, futures, list_expried, list_ids, list_new, list_old, list_update, operations, res, result, results, results_order, row, s
        except:
            pass

        stamp6 = datetime.datetime.now(datetime.timezone.utc)

        print('total used')
        countdown(stamp6, stamp1)
    else:
        print('dont too hurry :)')
# %% get_markets_history
get_markets_region_id_types = True
print(colored('\nget_markets_region_id_types', 'green'))
if get_markets_region_id_types is True:

    conn, c = lighter()
    c.execute('SELECT max(lastupdate) as last_update FROM tad_reg_order_history')
    latest = c.fetchone()[0]
    latest = datetime.datetime.strptime(''.join(latest.rsplit(':', 1)), '%Y-%m-%d %H:%M:%S.%f%z')

    now = datetime.datetime.now(datetime.timezone.utc)

    delta = now - latest
    print('since last update')
    countdown(now, latest)
    if delta.total_seconds() / 60 > 24 * 60:
    # if delta.total_seconds() / 60 > 1:

        stamp1 = datetime.datetime.now(datetime.timezone.utc)

        # 1
        stampA = datetime.datetime.now(datetime.timezone.utc)
        print('get url_cakes')
        region_ids = [
            # 10000012,  # curse
            10000005,  # Detorid
            # # 10000061,  # Tenerifis
            # # 10000009,  # Insmother
            # # 10000025,  # Immensea
            # # 10000006,  # Wicked Creek
            # # 10000008,  # Scalding Pass
            10000002  # the forge
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

                url = 'https://esi.evetech.net/latest/markets/{}/history/?datasource=tranquility&type_id={}'.format(
                    str(region),
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
                # future = session.get(cake['url'])
                #
                #
                # # test
                # res=future.result()
                #
                #
                #
                # df = pd.io.json.json_normalize(json.loads(res.content))
                # df['type_id'] = id
                # df['region_id'] = region
                #
                # try:
                #     dfs
                # except:
                #     dfs = df
                #
                # try:
                #     dfs = dfs.append(df, ignore_index=True, sort=False)
                # except Exception as e:
                #     print(colored('Error: {}\n'.format(e), 'green'))

                url_cakes.append(cake)

        stampB = datetime.datetime.now(datetime.timezone.utc)
        countdown(stampB, stampA)

        # 2
        stampA = datetime.datetime.now(datetime.timezone.utc)
        print('get url')
        with FuturesSession(executor=ThreadPoolExecutor(max_workers=None)) as session:

            futures = [session.get(cake['url']) for cake in url_cakes]

            results = []
            for result in as_completed(futures):
                results.append(result)

        stampB = datetime.datetime.now(datetime.timezone.utc)

        countdown(stampB, stampA)


        # 3

        def feeling(result):
            content = result.result().content

            url = result.result().url

            nn = re.findall(r'\d+', url)

            dict = json.loads(content)

            for df in dict:
                df['type_id'] = nn[1]
                df['region_id'] = nn[0]
                df['lastupdate'] = now
                big_dick.append(df)


        now = str(datetime.datetime.now(datetime.timezone.utc))
        stampA = datetime.datetime.now(datetime.timezone.utc)
        print('clear data')
        with ThreadPoolExecutor(max_workers=None) as executor:
            big_dick = []
            executor.map(feeling, results)

        stampB = datetime.datetime.now(datetime.timezone.utc)

        countdown(stampB, stampA)

        # 4

        stampA = datetime.datetime.now(datetime.timezone.utc)
        print('data 2 pd')
        df = pd.DataFrame(big_dick)
        stampB = datetime.datetime.now(datetime.timezone.utc)

        countdown(stampB, stampA)

        # stampA = datetime.datetime.now(datetime.timezone.utc)
        print('pd 2 sql')

        tablename = 'tad_reg_order_history'

        stampin = datetime.datetime.now(datetime.timezone.utc)
        conn, c = lighter()
        c.execute('TRUNCATE ONLY %s' % tablename)
        conn.commit()
        print('TRUNCATED')

        try:
            del output
        except:
            pass
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        # contents = output.getvalue()
        c.copy_from(output, tablename, null="")  # null values become ''
        conn.commit()
        conn.close()
        c.close()
        stampout = datetime.datetime.now(datetime.timezone.utc)
        print('insert completed')
        countdown(stampout, stampin)

        # df_columns = list(df)
        #
        # # create (col1,col2,...)
        # columns = ",".join(df_columns)
        #
        # # create VALUES('%s', '%s",...) one '%s' per column
        # values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        #
        # # create INSERT INTO table (columns) VALUES('%s',...)
        # insert_stmt = "INSERT INTO {} ({}) {}".format(tablename, columns, values)
        #
        # psycopg2.extras.execute_batch(c, insert_stmt, df.values)
        # conn.commit()
        # c.close()
        # stampout = datetime.datetime.now(datetime.timezone.utc)
        # print('insert completed')
        # countdown(stampout, stampin)

        # df.to_sql('tad_reg_order_history', con=engine, index=False, if_exists='replace', chunksize=5000,
        #           dtype={'lastupdate': sqlalchemy.types.TIMESTAMP(timezone=True)
        #                  })

        # df.head(0).to_sql('tad_reg_order_history', con=engine, if_exists='replace', index=False)  # truncates the table
        #
        # conne = engine.raw_connection()
        # cur = conne.cursor()
        # output = io.StringIO()
        # df.to_csv(output, sep='\t', header=False, index=False)
        # output.seek(0)
        # # contents = output.getvalue()
        # cur.copy_from(output, 'tad_orders_temp', null="")  # null values become ''
        # conne.commit()
        # conne.close()

        # stampB = datetime.datetime.now(datetime.timezone.utc)
        #
        # countdown(stampB, stampA)

        #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # # def cookcakes(cake):
        # #     url = cake['url']
        # #     region = cake['region']
        # #     id = cake['type_id']
        # #     # esi=requests.Session()
        # #
        # #     for i in range(0, 5):
        # #
        # #         try:
        # #             res = ESI_SESSION.get(url)
        # #
        # #             # if res.status_code != 420:
        # #             #     print(res.status_code,'try+ ', i+1, '\nsleeping')
        # #             #     time.sleep(5)
        # #             #     continue
        # #
        # #             if res.status_code != 200:
        # #                 # print(res.status_code,'try+ ', i+1, '\nsleeping')
        # #                 time.sleep(3)
        # #                 try:
        # #                     e_remain = int(res.headers.get("x-esi-error-limit-remain"))
        # #                     e_reset = int(res.headers.get("x-esi-error-limit-reset"))
        # #                     if e_remain < 50:
        # #                         print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
        # #                         print('sleep {}s'.format(e_reset))
        # #                         time.sleep(e_reset)
        # #                 except:
        # #                     break
        # #                 continue
        # #         except Exception as e:
        # #             print('Error:' + str(e) + '\nsleeping')
        # #             time.sleep(3)
        # #
        # #             e_remain = int(res.headers.get("x-esi-error-limit-remain"))
        # #             e_reset = int(res.headers.get("x-esi-error-limit-reset"))
        # #             if e_remain < 50:
        # #                 print('WARNING: x-esi-error-limit-remain {}'.format(e_remain))
        # #                 print('sleep {}s'.format(e_reset))
        # #                 time.sleep(e_reset)
        # #
        # #             if res.status_code > 400:
        # #                 print(res.status_code)
        # #             continue
        # #         break
        # #
        # #     if res.status_code == 200:
        # #         df = pd.io.json.json_normalize(json.loads(res.content))
        # #         df['type_id'] = id
        # #         df['region_id'] = region
        # #
        # #         return df
        # #
        # #         # try:
        # #         #     dfs = dfs.append(df, ignore_index=True, sort=False)
        # #         # except:
        # #         #     dfs = df
        # #     else:
        # #         print(res.status_code, colored('lost\n', 'red'))
        # #
        # #         return {}
        # #
        # #
        # # print('shake handing')
        # # # try:
        # # #     del dfs
        # # # except:
        # # #     pass
        # # #
        # # # l = len(url_cakes)
        # # # with ThreadPoolExecutor(max_workers=200) as executor:
        # # #     stamp3 = datetime.datetime.now(datetime.timezone.utc)
        # # #     ESI_SESSION = requests.Session()
        # # #     futures = [executor.submit(cookcakes, cake) for cake in url_cakes]
        # # #
        # # #     for idx, result in enumerate(as_completed(futures)):
        # # #
        # # #         try:
        # # #             dfs
        # # #         except:
        # # #             dfs = result._result
        # # #
        # # #         try:
        # # #             dfs = dfs.append(result._result, ignore_index=True, sort=False)
        # # #         except:
        # # #             pass
        # # #
        # # #         print('{} of {:06.2f} % '.format(idx, idx / l * 100), end='')
        # # #
        # # # stamp4 = datetime.datetime.now(datetime.timezone.utc)
        # # # countdown(stamp4, stamp3)
        # #
        # # session = FuturesSession(executor=ThreadPoolExecutor(max_workers=10))
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # #
        # # stamp3 = datetime.datetime.now(datetime.timezone.utc)
        # # with FuturesSession(executor=ThreadPoolExecutor(max_workers=None)) as session:
        # #
        # #    # futures= [zip(session.get(cake['url']), cake['region'], cake['type_id']) for cake in url_cakes]
        # #
        # #    for cake in url_cakes:
        # #        cake['res']=session.get(cake['url'])
        # #
        # #    stamp4 = datetime.datetime.now(datetime.timezone.utc)
        # #    countdown(stamp4, stamp3)
        # #
        # #    l = len(url_cakes)
        # #    a = 0
        # #    for cake in url_cakes:
        # #        if cake['res']._exception == None:
        # #
        # #            df = pd.io.json.json_normalize(json.loads(cake['res'].result().content))
        # #            a += 1
        # #            print('{} done {:06.2f} % '.format(a, a / l * 100), end='')
        # #            df['type_id'] = id
        # #            df['region_id'] = region
        # #            try:
        # #                dfs
        # #            except:
        # #                dfs = df
        # #
        # #            try:
        # #                dfs = dfs.append(df, ignore_index=True, sort=False)
        # #            except Exception as e:
        # #                print(colored('Error: {}\n'.format(e), 'green'))
        # #                continue
        # #        else:
        # #            print(colored( cake['res']._exception,'red'))
        # #
        # # stamp5= datetime.datetime.now(datetime.timezone.utc)
        # # countdown(stamp5, stamp4)
        # #
        # # # results = []
        # # # for result in as_completed(futures):
        # # #      # if result.result().status_code ==200:
        # # #      #     # if len(result.result().content)> 2:
        # # #      #     #     df = pd.io.json.json_normalize(json.loads(result.result().content))
        # # #      #     #     try:
        # # #      #     #         dfs = dfs.append(df, ignore_index=True, sort=False)
        # # #      #     #     except:
        # # #      #     #         dfs = df
        # # #      #
        # # #      #     results.append(result.result().content)
        # # #      # else:
        # # #      #     print(result.result().status_code)
        # # #      results.append(result)
        # #
        # # # futures= [session.get(cake['url'], background=bg_cb(type_id=cake['type_id'],region=cake['region'])) for cake in url_cakes]
        # #
        # # #     stamp2 = datetime.datetime.now(datetime.timezone.utc)
        # # #     results=[]
        # # #     futures= [session.get(cake['url'], background=bg_cb(type_id=cake['type_id'],region=cake['region'])) for cake in url_cakes]
        # # #
        # # #     results=[]
        # # #     for result in as_completed(futures):
        # # #         # if result.result().status_code ==200:
        # # #         #     # if len(result.result().content)> 2:
        # # #         #     #     df = pd.io.json.json_normalize(json.loads(result.result().content))
        # # #         #     #     try:
        # # #         #     #         dfs = dfs.append(df, ignore_index=True, sort=False)
        # # #         #     #     except:
        # # #         #     #         dfs = df
        # # #         #
        # # #         #     results.append(result.result().content)
        # # #         # else:
        # # #         #     print(result.result().status_code)
        # # #         results.append(result)
        # # #     stamp3 = datetime.datetime.now(datetime.timezone.utc)
        # # #     countdown(stamp3, stamp2)
        # #
        # # ### insert into db
        # print('insert the database')
        # tablename = 'tad_reg_order_history'
        # now = datetime.datetime.now(datetime.timezone.utc)
        #
        # # dict = dfs.drop_duplicates().to_dict(orient='records')
        # stamp5 = datetime.datetime.now(datetime.timezone.utc)
        # conn, c = lighter()
        #
        # c.execute('select count(*) from %s' % (tablename))
        # before = c.fetchone()[0]
        #
        # dfs['lastupdate']=now
        # dfs.to_sql(tablename, con=engine, index=False, if_exists='replace')
        #
        # # def cookdb(results):
        # # for result in results:
        # #     dict = result._result
        # #     if dict is not None:
        # #         if len(dict) > 0:
        # #             for row in dict:
        # #                 # print(row)
        # #                 # tag_date = 'date_issued'
        # #                 # if isinstance(row[tag_date], datetime.date) is False:  # make sure it is datatime
        # #                 #     row[tag_date] = datetime.datetime.strptime(row[tag_date], '%Y-%m-%dT%H:%M:%SZ').replace(
        # #                 #         tzinfo=datetime.timezone.utc)
        # #
        # #                 tag_date = 'date'
        # #                 if isinstance(row[tag_date], datetime.date) is False:  # make sure it is datatime
        # #                     row[tag_date] = datetime.datetime.strptime(row[tag_date], '%Y-%m-%d')
        # #
        # #                 tag_int = ['order_count', 'volume']
        # #                 for tag in tag_int:
        # #                     if isinstance(row[tag], bool) is False:
        # #                         if row[tag] > 0:
        # #                             row[tag] = int(row[tag])
        # #                         else:
        # #                             row[tag] = None
        # #                 #
        # #                 # tag_bool = 'is_blueprint_copy'
        # #                 # if isinstance(row[tag_bool], bool) is False:
        # #                 #     if row[tag_bool] > 0:
        # #                 #         row[tag_bool] = True
        # #                 #     else:
        # #                 #         row[tag_bool] = False
        # #                 #
        # #                 # tag_float = 'material_efficiency'
        # #                 # if isinstance(row[tag_float], float) is False:
        # #                 #     row[tag_float] = float(row[tag_float])
        # #
        # #                 row['last_update'] = now
        # #                 # print('')
        # #                 # for item in row:
        # #                 #     print(row[item])
        # #
        # #                 tag_unique = ('type_id', 'region_id', 'date')
        #
        # #                 sql = '''INSERT INTO %s (%s)
        # #                                VALUES ( %%(%s)s )
        # #                                ON CONFLICT (%s)
        # #                                DO NOTHING
        # #                                    ''' % (
        # #                     tablename, ',  '.join(row), ')s, %('.join(row), ',  '.join(tag_unique))
        # #                 # print(sql)
        # #                 c.execute(sql, row)
        # #
        # #     # with ThreadPoolExecutor(max_workers=None) as executor:
        # #     #     stamp4 = datetime.datetime.now(datetime.timezone.utc)
        # #     #
        # #     #
        # #     #     executor.map(cookdb, results)
        # #     #
        # #     #     # futures = [executor.submit(cookcakes, cake) for cake in url_cakes]
        # #     #     # results = []
        # #     #     # for result in as_completed(futures):
        # #     #     #     results.append(result)
        # #     #
        # #     #     stamp5 = datetime.datetime.now(datetime.timezone.utc)
        # #     #     countdown(stamp3, stamp2)
        # #
        # # conn.commit()
        #
        # c.execute('select count(*) from %s' % (tablename))
        # after = c.fetchone()[0]
        # print(colored('{} new records in {}'.format(after - before, tablename), 'green'))
        stamp2 = datetime.datetime.now(datetime.timezone.utc)
        countdown(stamp2, stamp1)
    else:
        print('not yet updated in real world')


# %% start to analysis
def ana():
    ## find the type_id list for det

    target_reg = [
        10000002,  # the forge
        10000005,  # Detorid
        # 10000006,  # Wicked Creek
        # 10000008,  # Scalding Pass
        # 10000009,  # Insmother
        # 10000012,  # Curse
        # 10000025,  # Immensea
        # 10000061  # Tenerifis
    ]
    # find T2 things exist in det

    # df_ids=pd.read_sql('universe_ids', con=engine)
    #
    # df_pub_ord=pd.read_sql('tad_reg_pub_orders', con=engine)
    #
    # # df_det_sys=pd.read_sql('SELECT system_name, system_id from universe_geo WHERE region_id')
    #
    # df_ord_det=df_pub_ord.loc[df_pub_ord['system_id']==30000481]
    #
    # df_ord_jita=df_pub_ord.loc[df_pub_ord['system_id']==30000142]
    #
    # df_t2 = pd.read_sql('SELECT * FROM universe_ids WHERE metalevel=5', con=engine)
    #
    # # get systems for "1026996997751" QRF
    #
    # df_geo_det = pd.read_sql('SELECT system_id FROM universe_geo where region_id=10000005', con=engine)
    #
    # df_stations=pd.read_sql('universe_stations_temp', con=engine)
    #
    # df_stations['system_id_n']=df_stations['system_id']+df_stations['solar_system_id']
    #
    # df_stations_det=df_stations[df_stations['system_id_n'].isin(df_geo_det['system_id'].tolist())]
    #
    # # get orders from det
    #
    # df_ord_det = pd.read_sql(
    #     'SELECT * FROM tad_orders_temp where location_id in {}'.format(tuple(df_stations_det['location_id'].tolist())), con=engine)
    # # get T2 orders
    # df_ord_det_t2 = df_ord_det[df_ord_det['type_id'].isin(df_t2['type_id'].tolist())]
    # df_ord_det_t2=pd.merge(df_ord_det_t2,df_t2,on='type_id', how='left')
    # print(df_ord_det_t2.head())
    #
    # df=df_ord_det_t2.groupby(['type_id','location_id'], as_index=False).agg({'volume_remain':{'total':'sum'},
    #                                                       'price':{'min':'min','max':'max'},
    #                                                       'order_id':{'count':'count'}
    #                                                       })
    #
    # print(df.head())

    df_ids = pd.read_sql('universe_ids', con=engine)

    df_ids = df_ids[['type_id', 'type_name', 'group_name',
                     'cat_name', 'packaged_volume', 'metalevel', 'techlevel',
                     'metagroup']]

    df_geo = pd.read_sql('universe_geo', con=engine)

    df_geo = df_geo[['system_id', 'constellation_id', 'region_id', 'system_name', 'region_name']]

    df_stations = pd.read_sql('universe_stations_temp', con=engine)

    df_stations['system_id'] = df_stations['system_id'] + df_stations['solar_system_id']

    df_stations = df_stations[['name', 'system_id', 'pos_tpye', 'location_id']]

    df_ords = pd.read_sql('tad_orders_temp', con=engine)

    anly = df_ords[df_ords['location_id'].isin([1026996997751,  # QRFJ
                                                60003760  # JITA
                                                ])]

    # print(anly.head())

    anly = pd.merge(anly, df_ids, on='type_id')

    anly = pd.merge(anly, df_stations, on='location_id')

    sell = anly[anly['is_buy_order'] == False]
    buy = anly[anly['is_buy_order'] == True]

    # df=buy.groupby(['type_id'], as_index=False).agg({'volume_remain':{'total':'sum'},
    #                                                       'price':{'min':'min','max':'max'},
    #                                                       'order_id':{'count':'count'}
    #                                                       })

    anly_buy = buy.groupby(['type_id', 'type_name', 'cat_name', 'name'], as_index=False).agg({'volume_remain': 'sum',
                                                                                              'price': ['min', 'max'],
                                                                                              'order_id': 'count'
                                                                                              })
    anly_buy.columns = ["_".join(x) for x in anly_buy.columns.ravel()]

    anly_buy_qrfj = anly_buy[anly_buy['name_'] == 'QRFJ-Q - WC starcity']

    anly_buy_jita = anly_buy[anly_buy['name_'] != 'QRFJ-Q - WC starcity']

    anly_sell = sell.groupby(['type_id', 'type_name', 'cat_name', 'name'], as_index=False).agg({'volume_remain': 'sum',
                                                                                                'price': ['min', 'max'],
                                                                                                'order_id': 'count'
                                                                                                })

    anly_sell.columns = ["_".join(x) for x in anly_sell.columns.ravel()]

    anly_sell_qrfj = anly_sell[anly_sell['name_'] == 'QRFJ-Q - WC starcity']

    anly_sell_jita = anly_sell[anly_sell['name_'] != 'QRFJ-Q - WC starcity']

    # Export
    # 1
    df_q2j_s2b = pd.merge(anly_sell_qrfj, anly_buy_jita, on='type_id_')
    df_q2j_s2b['raw_profit_q2j_s2b'] = df_q2j_s2b['price_max_y'] - df_q2j_s2b['price_min_x']
    df_q2j_s2b = pd.merge(df_q2j_s2b, df_ids, left_on='type_id_', right_on='type_id')
    df_q2j_s2b['raw_margin_q2j_s2b'] = (df_q2j_s2b['price_max_y'] - df_q2j_s2b['price_min_x']) / df_q2j_s2b[
        'price_min_x']
    df_q2j_s2b = df_q2j_s2b.sort_values(by=['raw_margin_q2j_s2b'], ascending=False)

    df_q2j_s2b.to_sql('df_q2j_s2b', con=engine, index=False, if_exists='replace')

    # 2
    df_q2j_s2s = pd.merge(anly_sell_qrfj, anly_sell_jita, on='type_id_')
    df_q2j_s2s['raw_profit-q2j_s2s'] = df_q2j_s2s['price_min_y'] - df_q2j_s2s['price_min_x']
    df_q2j_s2s = pd.merge(df_q2j_s2s, df_ids, left_on='type_id_', right_on='type_id')
    df_q2j_s2s['raw_margin-q2j_s2s'] = (df_q2j_s2s['price_min_y'] - df_q2j_s2s['price_min_x']) / df_q2j_s2s[
        'price_min_x']
    df_q2j_s2s = df_q2j_s2s.sort_values(by=['raw_margin-q2j_s2s'], ascending=False)

    df_q2j_s2s.to_sql('df_q2j_s2s', con=engine, index=False, if_exists='replace')

    # import
    # 3
    df_j2q_b2s = pd.merge(anly_buy_jita, anly_sell_qrfj, on='type_id_')
    df_j2q_b2s['raw_profit_j2q_b2s'] = df_j2q_b2s['price_min_y'] - df_j2q_b2s['price_max_x']
    df_j2q_b2s = pd.merge(df_j2q_b2s, df_ids, left_on='type_id_', right_on='type_id')
    df_j2q_b2s['raw_margin_j2q_b2s'] = df_j2q_b2s['raw_profit_j2q_b2s'] / df_j2q_b2s['price_max_x']
    df_j2q_b2s['profit-ship_j2q_b2s'] = df_j2q_b2s['raw_profit_j2q_b2s'] - df_j2q_b2s['packaged_volume'] * 1400
    df_j2q_b2s['margin-ship_j2q_b2s'] = df_j2q_b2s['profit-ship_j2q_b2s'] / df_j2q_b2s['price_min_x']
    df_j2q_b2s = df_j2q_b2s.sort_values(by=['margin-ship_j2q_b2s'], ascending=False)

    df_j2q_b2s.to_sql('df_j2q_b2s', con=engine, index=False, if_exists='replace')

    # 4
    df_j2q_s2s = pd.merge(anly_sell_jita, anly_sell_qrfj, on='type_id_')
    df_j2q_s2s['raw_profit_j2q_s2s'] = df_j2q_s2s['price_min_y'] - df_j2q_s2s['price_min_x']
    df_j2q_s2s = pd.merge(df_j2q_s2s, df_ids, left_on='type_id_', right_on='type_id')
    df_j2q_s2s['raw_margin_j2q_s2s'] = df_j2q_s2s['raw_profit_j2q_s2s'] / df_j2q_s2s['price_min_x']
    df_j2q_s2s['profit-ship_j2q_s2s'] = df_j2q_s2s['raw_profit_j2q_s2s'] - df_j2q_s2s['packaged_volume'] * 1400
    df_j2q_s2s['margin-ship_j2q_s2s'] = df_j2q_s2s['profit-ship_j2q_s2s'] / df_j2q_s2s['price_min_x']
    df_j2q_s2s = df_j2q_s2s.sort_values(by=['margin-ship_j2q_s2s'], ascending=False)

    df_j2q_s2s.to_sql('df_j2q_s2s', con=engine, index=False, if_exists='replace')


# %% end
stamp99 = datetime.datetime.now(datetime.timezone.utc)
print(colored('\nMission Complete', 'green'))

countdown(stamp99, stamp00)
