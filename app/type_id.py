#%% get all page of type_id

op = app.op['get_universe_types'](page=1)
response = client.request(op)

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




#%% GET all content of type_id
operations = []
for page in results:
    for t_id in page[1].data:
        operations.append(app.op['get_universe_types_type_id'](type_id=t_id))


res_id = client.multi_request(operations)
# op = app.op['get_universe_types_type_id'](type_id=48190)




#%% Extract the data to DF
itemDB=dict()
for item in res_id:
    d = json.loads(item[1].raw)
    t_id = d['type_id']
    itemDB[t_id]=d

df_items=pd.DataFrame.from_dict(itemDB, orient='index')

df_items.to_csv('df_itemDB.csv')


