# eve play and learn project

created: 2018-05-02

last edit: 2018-06-22

## Situation

EVE is the best game for data science people, because CCP(game producer) applied very fashion technology to share the data of this game.

Question is can we do something for the game data?  If we have the data, can we use them wisely?

There are various data, and enabled diverse approaches to use the data. Here we select one path that we think would be fun, useful, and not too hard to finish.

### Case study

Speaking about data, the first thing come to head is the market data. If we can access enough data in short time, then we could identify which item to trade, which would make the max profit.

Current (2018-05), there already exist some trading tools, such as [EVERNUS](https://evernus.com/), [evetrade](https://evetrade.space/),[eveprasial](http://evepraisal.com/) etc.

After having these tools, maybe for [station trading](https://wiki.braveineve.com/public/dojo/wiki/station_trading_complete_guide) is almost enough, but for interregional hauling, still far from enough.

The major pain points are:

- Data may be too big if cover multiple region ( *I start to doubt the size of data, can be quite tiny BS 20180622* )

- The analysis is not accurate, because interregional hauling involves more complex background and rules   (*lack both of the software and the mindset*)

What we want is to make a tool which can help you check the market state, make sure the item which you will hauling would make profit, and check the security state of the relevant routing, and plan the more safe or more short route for you to travel.

**All in one line, we want to develop a tool help you hauling between Jita and nullsec corporation centre (e.g. OSY). So everyone in the corporation would get better supply, and you will make more ISK, lost fewer ships.**

## Target

As mentioned in the case study, we want to develop a tool which can drag the data from CCP's API, and do some data analysis, provide a solution.

Here I should notice some other thing, that the target of this project is not really delivering some product (tool/software), is to have fun and play the game as a geek way.

By join this project, you will:

- Know a bit more about EVE online

- Gain know-how about trading and hauling

- practice the use of API (RESTful)

- practice the use of database (not decided yet, SQLite? MONGO DB? AWS things?)

- practice data processing method

- practice frontend UI design

## Action

1. Benchmarking study: know what relevant tools exist, who are the developers, how did they develop the tools, who and what is the best/ state-of-art. What we can learn from them.

2. Business content study: You can not do right thing if you also have data but not know enough about what the data mean. So you need know the business model in EVE or from real world

3. Design the architecture, classes and functions

4. Coding

5. Testing

6. Release

## Result (Working in Progress)

2018-05-17:  current I have done some benchmarking work, and I have experienced the station trading for a while

2018-06-22:
- Put hands on coding (Python)
- Already applied [EsiPy](https://kyria.github.io/EsiPy/) to collect the data from ESI, also conduct the SSO authentication process
- Prepared MySQL,,, could be overkill. Up to now, I haven't found the enough reason to use MySQL,,, (.csv, .txt could manage this size of data) maybe in future if I found a case that have to store massive data I will back to SQL.
- Using [Pandas](https://pandas.pydata.org/) dataframe to operate the data
- Finish the analysis process for chars' transaction history


Nest steps:
- analysis orders in one station
- analysis orders in multiple station
- find insight for hauling

## Team Up

Now we have 3-5 players are interested for this project.

We are expecting to have a team around 3-6 members.

The only requirement is [Interested in both game content and tech content]

Prefer have some tech background of web dev, data science

### How to join us

contact `hidesoon@gmail.com`



# How to use this tool
1. [SSO Login using EsiPy](https://kyria.github.io/EsiPy/examples/sso_login_esipy/)
  - APP key:
  you need go to CCP official developer space to create the API key for your application
  (you can use mine for this app,,,, if you need)
  Write your app key as `appkey.txt`
  - SSO token:
  this must be yours,,, SSO key can access all data from you character(s)
  Write the SSO token as `tokens.txt`

  2. Load the main.py
  (remember to call the functions )

  3. Run trans_anal.py (very sexy name, I know)

# Business questions
Q:What is trading in EVE?
A:
- Station trading
- hauling (interregional trading)

Q: What item is good for trading?
A: there is no fixed answer, you have to do market analysis


Q: What is the work flow for trading?
A:
BUY IN
1. Rise `Purchase Order`, pay `escrow`, `broker fee`, in `station`
2. Seller fulfilled your `Purchase orders` by `volume`, you pay rest of item value to seller, pay `transaction tax`
3. Item move in your `item hanger`, become stock/inventory or also called `Working in Progress (WIP)`

SELL OUT
4. Rise `Sell Order` ,pay  `broker fee`, in `station`
5. Buyer fulfilled your `sell order` with `volume`, buyer pay you rest of item value, pay `transaction tax`


Q: What is called `margin` or `markup`?

Q: How many quantity of this item should I buy?

Q: How many margin I can get from trading this item?

Q: 
