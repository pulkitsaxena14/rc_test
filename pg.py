from sqlalchemy import create_engine
import pandas as pd

def connect_pg():
    engine = create_engine('postgresql://test_user:testtest@localhost/redcarpet')
    return engine

def create_db(location_csv):
    eng = connect_pg()
    c = eng.connect()
    df1 = pd.read_csv(location_csv)
    df1.to_sql('zeta_wallet', eng, if_exists='append', index=False)
    c.close()

def test_create_db():
    loc = '/home/saxena/PycharmProjects/test1/Tests/Zeta_Wallet_Loads_Zeta_Loads.csv'
    create_db(loc)
    eng = connect_pg()
    c = eng.connect()
    df1 = pd.read_sql_table('zeta_wallet', eng)
    c.close()
    df2 = pd.read_csv(loc)
    assert (df1.head()).equals(df2.head())