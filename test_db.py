import os
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

usr = os.environ.get('USR')
pw = os.environ.get('PW')

alchemyEngine   = create_engine(f'postgresql+psycopg2://{usr}:{pw}@127.0.0.1/marketdata');
dbConnection    = alchemyEngine.connect();

print("before")
df = pd.read_sql("SELECT * FROM test", dbConnection);
print(df.head())

df_app = pd.DataFrame({'label': ['a', 'b', 'c']})
df_app.to_sql('test', dbConnection, if_exists='append', index=False)

print("after")
df = pd.read_sql("SELECT * FROM test", dbConnection);
print(df.head())