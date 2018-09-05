# %% Read data
# from structure order reading
try:
    df_str = pd.read_csv('dt_structure_orders_winter.csv')
except Exception as e:
    print('Error:' + str(e) + "\n")
# from region order reading
try:
    df_reg = pd.read_csv('df_regional_orders.csv')
except Exception as e:
    print('Error:' + str(e) + "\n")

try:
    filename = 'dt_locations.csv'

    df_loc = pd.read_csv(filename).drop(columns='Unnamed: 0')
    list_loc= df_loc['location_id'].tolist()
except Exception as e:
    print('Error:' + str(e) + "\n")

# %% split Winter and JITA orders

df_winter = df_reg[df_reg['location_id'].isin(list_loc)]

df_jita = df_reg[df_reg['location_id'].isin([
    60003760,  # Jita IV - Moon 4 - Caldari Navy Assembly Plant
    1023164547009  # Perimeter - - IChooseYou Trade Hub
])]

df_jita_sell = df_jita[df_jita['is_buy_order'] == False]

# %% Merge OSY to df_str

df_winter = df_str.append(df_winter, ignore_index=True, sort=False)

# look at selling
df_winter_sell = df_winter[df_winter['is_buy_order'] == False]

winter_type_ids = df_winter_sell.type_id.unique()

# %% check the lowest price of the item
import_list = []
export_list = []
df_shoppinglist = pd.DataFrame(
    columns=('type_id', 'jita.s', 'frt.s', 'is_import', 'margin', 'proft_pu', 'volume', 'total_profit'))

for id in winter_type_ids:
    # print(id)
    # FRT
    df_tpye_n = df_winter_sell[df_winter_sell['type_id'] == id]
    # rr,cc=df_tpye.shape
    # print(rr, cc)
    min_price_n = df_tpye_n.price.min()
    df_tpye_frt = df_tpye_n[df_tpye_n.price.between(min_price_n, min_price_n / 0.90)]  # valid orders

    # JITA
    df_tpye_h = df_jita_sell[df_jita_sell['type_id'] == id]
    min_price_h = df_tpye_h.price.min()
    df_tpye_jita = df_tpye_h[df_tpye_h.price.between(min_price_h, min_price_h / 0.90)]  # valid orders

    # gap
    gap = min_price_n - min_price_h
    if gap > 0:
        trade_import = 1
    else:
        trade_import = 0

    if trade_import == 1:
        # margin = gap / min_price_h
        # print('import from jita')
        # def f(row):
        #     return row['volume_remain'] * row['price']
        # volume= df_tpye_frt.volume.remain.sum()
        #
        # cost=df_tpye_frt.apply(f, axis=1)
        # cost=cost.sum()
        margin_ideal = 0.1

        margin = gap / min_price_h

        if margin > margin_ideal:
            # how many to buy?
            volume = df_tpye_frt.volume_total.sum()
            profit_total = gap * volume
            if profit_total > 50000000:
                print('{} Import, jita.s {}, frt.s {} margin: {}, qty: {}, total profit: {}'.format(id, min_price_h,
                                                                                                    min_price_n, margin,
                                                                                                    volume,
                                                                                                    profit_total))
                for order in df_tpye_frt.order_id.values:
                    import_list.append(order)
                dft = pd.Series({'type_id': id, 'jita.s': min_price_h, 'frt.s': min_price_n,
                                 'is_import': trade_import, 'margin': margin, 'proft_pu': gap, 'volume': volume,
                                 'total_profit': profit_total})
                df_shoppinglist = df_shoppinglist.append(dft, ignore_index=True)

    else:
        margin_ideal = 0.1

        margin = gap / min_price_n * (-1)
        if margin > margin_ideal:
            def f(row):
                return row['volume_remain'] * row['price']


            volume = df_tpye_frt.volume_remain.sum()

            cost = df_tpye_frt.apply(f, axis=1)
            cost = cost.sum()

            profit_total = min_price_h * volume - cost
            if profit_total > 10000000:

                print(
                    '{} Export, jita.s {}, frt.s {} margin: {}, qty: {}, total profit: {}'.format(id, min_price_h,
                                                                                                  min_price_n,
                                                                                                  margin, volume,
                                                                                                  profit_total))
                for order in df_tpye_frt.order_id.values:
                    export_list.append(order)
                dft = pd.Series({'type_id': id, 'jita.s': min_price_h, 'frt.s': min_price_n,
                                 'is_import': trade_import, 'margin': margin, 'proft_pu': gap, 'volume': volume,
                                 'total_profit': profit_total})
                df_shoppinglist = df_shoppinglist.append(dft, ignore_index=True)

df_winter_import = df_winter[df_winter['order_id'].isin(import_list)]
df_winter_import['is_import'] = True
df_winter_export = df_winter[df_winter['order_id'].isin(export_list)]
df_winter_export['is_import'] = False

# %% write to csv
df = pd.concat([df_winter_export, df_winter_import], ignore_index=True, sort=False)
df.to_csv('shoppinglist_order_id.csv', encoding='utf_8_sig')
df_shoppinglist.to_csv('shoppinglist.csv', encoding='utf_8_sig')
