from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://test_user:testtest@localhost/redcarpet')
c = engine.connect()
df1 = pd.read_csv('/home/saxena/PycharmProjects/test1/Tests/Zeta_Wallet_Loads_Zeta_Loads.csv')

if engine.has_table('zeta_wallet') is False:
    df1.index += 1
    df1.to_sql('zeta_wallet', engine, if_exists='append', index=True)
    engine.execute("ALTER TABLE zeta_wallet ADD PRIMARY KEY(index)")
else:
    res = engine.execute('Select count(*) as cnt from zeta_wallet;')
    for i in res:
        df1.index += (i['cnt'] + 1)
    df1.to_sql('zeta_wallet', engine, if_exists='append', index=True)

df2 = pd.read_sql_table('zeta_wallet', engine)
df1.fillna(value='None', inplace=True)
df2.fillna(value='None', inplace=True)
#print(result)
#print(df2.index)
#print(-df1.isin(df2))

c.close()