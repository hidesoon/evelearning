# %% get the data
import pandas as pd

op = 'get_characters_character_id_wallet_transactions'
try:
    res = getdata(app, client, security, tokens, opcall=op, personal=1)
except Exception as e:
    print('Error:' + str(e) + "\n")
    # tokens, security = refresh_tokens(tokens, security)
    # res = getdata(app, client, security, opcall=op, personal=1)

# %% merge data


# if totally new, just create a file

df_now = pd.read_json(res.raw)

df_now = df_now[['transaction_id', 'journal_ref_id', 'client_id', 'date', 'is_buy', 'is_personal',
                 'location_id', 'quantity',
                 'type_id', 'unit_price']]
# df_now=df_now.set_index(['transaction_id','journal_ref_id'])


df_trans_mega = pd.read_csv('df_trans_mega.csv')
rr0, cc0 = df_trans_mega.shape

df_trans_mega = df_trans_mega[['transaction_id', 'journal_ref_id', 'client_id', 'date', 'is_buy', 'is_personal',
                               'location_id', 'quantity',
                               'type_id', 'unit_price']]

df_merged = pd.concat([df_now, df_trans_mega], ignore_index=True)

df_merged = df_merged.drop_duplicates(subset=['transaction_id', 'journal_ref_id'])

rr1, cc1 = df_merged.shape

print('There are {} rows in store, added {} new rows'.format(rr0, rr1 - rr0))

df_merged.to_csv('df_trans_mega.csv')

print('trans mega saved')

# %% remove non-trans data
api_info = security.verify()
my_id = api_info['CharacterID']

rr0, cc0 = df_merged.shape

df_filted = df_merged[df_merged['client_id'] != my_id]

rr1, cc1 = df_filted.shape

print('removed {} rows for my stupid selftrans'.format(rr0 - rr1))

NonTrading_items = {'plex': 44992, 'LargeSkillInjector': 40520}

for item in NonTrading_items:
    rr1, cc1 = df_filted.shape
    df_filted = df_filted[df_filted['type_id'] != NonTrading_items[item]]
    rr2, cc2 = df_filted.shape
    print('removed {} rows for NonTrading item {}'.format(rr1 - rr2, item))

# %% ISK per day
result = df_filted  # result is filtered res

# clean vars
list_days = []
sum_sell = 0
sum_buy = 0
the_old_day = None
i_row = 0

test = pd.DataFrame(columns=['date', 'sum_sell', 'sum_buy', 'margin'])

for index, row in result.iterrows():

    if row['type_id'] in NonTrading_items.values():  # ignore the none trading_items
        continue

    # the_day = row['date'].date()
    the_day = pd.to_datetime(row['date']).date()
    if the_old_day is None:  # anchor the current day
        the_old_day = the_day
        list_days.append(the_day)

    trans_sum = row['unit_price'] * row['quantity']  # sum up the total transaction value
    if row['is_buy'] is False:
        sum_sell = sum_sell + trans_sum
    else:
        sum_buy = sum_buy + trans_sum

    if the_day not in list_days:  # define the new day
        list_days.append(the_day)

        # print(the_day)
        # print(sum_sell)
        # print(sum_buy)
        margin = sum_sell - sum_buy  # calculate the profits for current day, write into file, clean the template
        print("In {}, income {:,.2f}, spend {:,.2f}, margin {:,.2f}".format(the_old_day, sum_sell, sum_buy, margin))
        test.loc[i_row] = pd.Series({'date': the_old_day, 'sum_sell': sum_sell, 'sum_buy': sum_buy, 'margin': margin})
        sum_sell = 0
        sum_buy = 0
        margin = 0
        i_row = i_row + 1
        the_old_day = the_day

print("margin per day is {:,.2f} ISK".format(test['margin'][
                                             1:].mean()))
# TODO: would the mean() be too simplified? should check the frist 5% value instead of mean

test.to_csv('df_trans_daily.csv')

# %% match names


df_type_DB = pd.read_csv('df_itemDB.csv',
                         header=0,
                         usecols=['type_id', 'name'])

result = pd.merge(result, df_type_DB, how='left', on='type_id')

# df_type_DB = df_type_DB[['capacity', 'description', 'group_id', 'mass', 'name',
#                          'packaged_volume', 'portion_size', 'published', 'radius', 'type_id',
#                          'volume', 'graphic_id', 'icon_id', 'dogma_attributes',
#                          'market_group_id', 'dogma_effects']]
#
# idkey=result.type_id.values
#
# namelist=list()
#
# for idk in idkey:
#     name=df_type_DB[df_type_DB['type_id']==idk]['name'].values[0]
#     namelist.append(name)
#
# result['item_name']=namelist

# TODO: Should sort the data based on the margin per day? or should write more readable recommendation promo

# %% trading analysis


trading_items_list = result['type_id'].unique()  # get a list of item that you have transaction history
df_items = pd.DataFrame(
    columns=['type_id', 'item_name', 'buy_total_qty', 'buy_ct', 'buy_mean_value', 'buy_std', 'buy_pd', 'sell_total_qty',
             'sell_ct',
             'sell_mean_value', 'sell_std', 'sell_pd', 'margin_pu', 'profit_pu', 'profit_pd', 'wip_pd', 'rare'])

for ind, item in enumerate(trading_items_list):
    name = df_type_DB.loc[df_type_DB['type_id'] == item, 'name']  # match name with type_id
    name=name.values[0]


    print(item, name)

    buy_total_qty = 0
    buy_ct = 0
    buy_mean_pic = 0
    buy_std_pic = 0
    buy_pd = 0
    sell_total_qty = 0
    sell_ct = 0
    sell_mean_pic = 0
    sell_std_pic = 0
    sell_pd = 0
    margin_pu = 0
    profit_pu = 0
    profit_pd = 0
    wip = 0
    rare = 0

    # The meaning of the rare code
    # 0: both sell and buy >1
    # 9: buy by 0-1 order, sell >1
    # -10: buy 0-1, sell 0-1
    # -19: buy >1 , sell 0-1

    # select this specific item by type_id
    df_item = result[result.type_id == item]

    # if Buy
    df_item_buy = df_item[df_item.is_buy == True]
    rr, cc = df_item_buy.shape

    if rr > 1:
        df_item_buy.date = pd.to_datetime(df_item_buy.date)

        df_item_buy = df_item_buy.sort_values(by='date')
        # print(df_item_buy.date)
        the_old_time = None
        diff_trans = None

        for index, row in df_item_buy.iterrows():

            if the_old_time is None:
                the_old_time = pd.to_datetime(row['date'])
                continue
            if diff_trans is None:
                if row['quantity'] == 0:
                    print('error on 199')
                    continue
                else:
                    diff_trans = (pd.to_datetime(row['date']) - the_old_time) / row['quantity']
                    the_old_time = pd.to_datetime(row['date'])
                    continue

            diff_trans = diff_trans + (pd.to_datetime(row['date']) - the_old_time) / row['quantity']
            the_old_time = pd.to_datetime(row['date'])
        trans_ct = diff_trans / (rr - 1)

        buy_ct = trans_ct / pd.Timedelta('1 hour')  # time unit is 1 hour
        buy_total_qty = df_item_buy.quantity.sum()
        buy_mean_pic = df_item_buy.unit_price.mean()  # TODO: does mean() is the most suitable method?
        buy_std_pic = df_item_buy.unit_price.std()
        if buy_ct==0:
            buy_pd=0
        else:
            buy_pd = 24 / buy_ct
        rare = rare + 1

    else:
        buy_ct = 0
        buy_total_qty = df_item_buy.quantity.sum()
        buy_mean_pic = df_item_buy.unit_price.mean()
        buy_std_pic = df_item_buy.unit_price.std()
        buy_pd = 0
        rare = rare + 10

    # print('buy_ct: {:6.2f},  buy_total_qty: {:4},  buy_mean_pic: {:,.2f},  buy_std: {:,.2f}'.format(buy_ct,
    #                                                                                                 buy_total_qty,
    #                                                                                                 buy_mean_pic,
    #                                                                                                 buy_std_pic))

    # Sell

    df_item_sell = df_item[df_item.is_buy == False]
    rr, cc = df_item_sell.shape

    if rr > 1:
        df_item_sell.date = pd.to_datetime(df_item_sell.date)
        df_item_sell = df_item_sell.sort_values(by='date')
        # print(df_item_sell.date)
        the_old_time = None
        diff_trans = None

        for index, row in df_item_sell.iterrows():

            if the_old_time is None:
                the_old_time = pd.to_datetime(row['date'])
                continue
            if diff_trans is None:
                diff_trans = (pd.to_datetime(row['date']) - the_old_time) / row['quantity']
                the_old_time = pd.to_datetime(row['date'])
                continue

            diff_trans = diff_trans + (pd.to_datetime(row['date']) - the_old_time) / row['quantity']
            the_old_time = pd.to_datetime(row['date'])
        trans_ct = diff_trans / (rr - 1)

        sell_ct = trans_ct / pd.Timedelta('1 hour')
        sell_total_qty = df_item_sell.quantity.sum()
        sell_mean_pic = df_item_sell.unit_price.mean()
        sell_std_pic = df_item_sell.unit_price.std()
        if sell_ct != 0:

            sell_pd = 24 / sell_ct
        else:
            sell_pd = 0
        rare = rare - 1
    else:
        sell_ct = 0
        sell_total_qty = df_item_sell.quantity.sum()
        sell_mean_pic = df_item_sell.unit_price.mean()
        sell_std_pic = df_item_sell.unit_price.std()
        sell_pd = 0
        rare = rare - 20

    # print('sell_ct: {:6.2f},  sell_total_qty: {:4},  sell_mean_pic: {:,.2f},  sell_std: {:,.2f}'.format(sell_ct,
    #                                                                                                     sell_total_qty,
    #                                                                                                     sell_mean_pic,
    #                                                                                                     sell_std_pic))

    bott = max(buy_ct, sell_ct)  # bottleneck is who need more ct (slower)
    if sell_ct - buy_ct != 0:
        wip = 24 / (sell_ct - buy_ct)
    else:
        wip = buy_total_qty - sell_total_qty

    if bott > 0:
        profit_pd = 24 / bott * sell_mean_pic - 24 / bott * buy_mean_pic
    else:
        profit_pd = sell_total_qty * sell_mean_pic - buy_total_qty * buy_mean_pic

    profit_pu = sell_mean_pic - buy_mean_pic

    if buy_mean_pic != 0:
        margin_pu = (sell_mean_pic - buy_mean_pic) / buy_mean_pic  # % of the margin
    else:
        margin_pu = 0

    df_items.loc[ind] = pd.Series({'type_id': item, 'item_name': name, 'buy_total_qty': buy_total_qty, 'buy_ct': buy_ct,
                                   'buy_mean_value': buy_mean_pic, 'buy_std': buy_std_pic, 'buy_pd': buy_pd,
                                   'sell_total_qty': sell_total_qty, 'sell_ct': sell_ct,
                                   'sell_mean_value': sell_mean_pic, 'sell_std': sell_std_pic, 'sell_pd': sell_pd,
                                   'margin_pu': margin_pu, 'profit_pu': profit_pu, 'profit_pd': profit_pd,
                                   'wip_pd': wip, 'rare': rare})
    # print('margin_pu = {:.2f}%'.format(margin_pu * 100))
    # print('profit_pu = {:,.2f}'.format(profit_pu))
    # print('profit_pd  = {:,.2f}'.format(profit_pd))
    # print('rare = {}'.format(rare))
    # print('wip = {:.2f}\n'.format(wip))

df_items.to_csv('df_items.csv')
print('Saved to csv')
