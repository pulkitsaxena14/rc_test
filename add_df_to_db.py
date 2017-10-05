from sqlalchemy import create_engine, MetaData, Column, DATE, INTEGER, Sequence, PrimaryKeyConstraint, TEXT, Table
import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

engine = create_engine('postgresql://test_user:testtest@localhost/redcarpet')
meta = MetaData(bind=engine)

connection = engine.connect()
zeta_wallet = Table('zeta_wallet', meta,
                    Column('wallet_id', INTEGER, Sequence('wallet_id_seq'), primary_key=True),
                    Column('DDate', TEXT, nullable=True),
                    Column('From_to_party', TEXT, nullable=True),
                    Column('Credit', TEXT, nullable=True),
                    Column('Debit', TEXT, nullable=True),
                    Column('Balance', TEXT, nullable=True),
                    Column('Bank_ref_txn_id', TEXT, nullable=True),
                    Column('Load', TEXT, nullable=True),
                    Column('Key', TEXT, nullable=True),
                    Column('Lender', TEXT, nullable=True),
                    Column('Zeta_Order_Id', TEXT, nullable=True),
                    Column('DMI_User_bal', TEXT, nullable=True),
                    Column('RC_User_bal', TEXT, nullable=True),
                    Column('DMI_usr_bal_post_may', TEXT, nullable=True),
                    Column('Chandan_comment', TEXT, nullable=True),
                    Column('Current_date', TEXT, nullable=True),
                    Column('Status', TEXT, nullable=True),
                    Column('Unnamed_col1', TEXT, nullable=True),
                    Column('Unnamed_col2', TEXT, nullable=True),
                    Column('Unnamed_col3', TEXT, nullable=True),
                    Column('Unnamed_col4', TEXT, nullable=True),
                    Column('Unnamed_col5', TEXT, nullable=True),
                    Column('Unnamed_col6', TEXT, nullable=True),
                    Column('Unnamed_col7', TEXT, nullable=True),
                    Column('Unnamed_col8', TEXT, nullable=True),
                    Column('Unnamed_col9', TEXT, nullable=True),
                    Column('Actual_bal', TEXT, nullable=True))


if engine.has_table('zeta_wallet') is False:
    zeta_wallet.create()

ins = zeta_wallet.insert()
df1 = pd.read_csv('/home/saxena/PycharmProjects/test1/Tests/Zeta_Wallet_Loads_Zeta_Loads.csv')
for tup in df1.itertuples():
    engine.execute(zeta_wallet.insert(), DDate=tup[1], From_to_party = tup[2], Credit = tup[3], Debit = tup[4], Balance = tup[5], Bank_ref_txn_id = tup[6], Load = tup[7], Key = tup[8], Lender = tup[9], Zeta_Order_Id = tup[10], DMI_User_bal = tup[11], RC_User_bal = tup[12], DMI_usr_bal_post_may = tup[13], Chandan_comment = tup[14], Current_date = tup[15], Status = tup[16], Unnamed_col1 = tup[17], Unnamed_col2 = tup[18], Unnamed_col3 = tup[19], Unnamed_col4 = tup[20], Unnamed_col5 = tup[21], Unnamed_col6 = tup[22], Unnamed_col7 = tup[23], Unnamed_col8 = tup[24], Unnamed_col9 = tup[25], Actual_bal = tup[26])
connection.close()