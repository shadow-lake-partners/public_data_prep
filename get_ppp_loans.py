# get_ppp_loans.py

import os
import sqlite3
import pandas as pd

# define list of urls
ppp_csv_url_list = [
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/501af711-1c91-477a-80ce-bf6428eb9253/download/public_150k_plus_220403.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/85f66605-568a-4400-aa5c-3803873153dc/download/public_up_to_150k_1_220403.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/783414f9-888b-47fb-b5dc-b1c0b102b6e5/download/public_up_to_150k_2_220403.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/148d5da2-c802-4c78-836e-6ee1d6c2bdf9/download/public_up_to_150k_3_220403.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/0fac3770-49da-4160-ae9f-53fc1b791c6e/download/public_up_to_150k_4_220403.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/b4ceb5fa-7693-43dc-af70-837cd88d84e9/download/public_up_to_150k_5_220403.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/980ec35b-d98c-452f-b4fc-2677dc923b90/download/public_up_to_150k_6_220403.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/61ef84ba-4e7c-4d19-96c9-761daed984d0/download/public_up_to_150k_7_220403.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/ba418afb-582d-444e-bf16-f73a9c0fea75/download/public_up_to_150k_8_220403.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/08de1369-982f-48a1-a828-256cb9ef488c/download/public_up_to_150k_9_220403.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/482b116e-35a6-4cd7-9171-e9dbe8b906eb/download/public_up_to_150k_10_220403.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/2c2adf29-a57c-4696-bfbf-452acd021eca/download/public_up_to_150k_11_220403.csv',
    'https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24/resource/0950ec12-6edc-4df3-acfc-b53a892e220d/download/public_up_to_150k_12_220403.csv'
]

# ------------------------------------ #
# Delete the DB if it exists
# ------------------------------------ #

if os.path.exists("ppp_loans.db"):
    os.remove("ppp_loans.db")

# ------------------------------------ #
# Make the db
# ------------------------------------ #

db = sqlite3.connect('ppp_loans.db')
cursor = db.cursor()
cursor.close()
db.close()

# ------------------------------------ #
# Make ppp table function
# ------------------------------------ #

def create_ppp_loans_table(csv_link:str):
    
    # Create a Dataframe to Read in the CSV 
    df = pd.read_csv(csv_link)
    df = df.astype(str)
    
    # rename the column titles to remove spaces and camelcase
    column_titles = []
    for title in df.columns:
        column_titles.append(title.replace(' ','_').replace('-','_').replace(',','').replace('.','').replace('/','_').replace('(','').replace(')','').lower())
    df.columns = column_titles

    # print check (optional)
    print(' ')
    print(csv_link)
    print(df.head())
    print(' ')    

    # ------------------------------------ #
    # Insert data to db 
    # ------------------------------------ #

    conn = sqlite3.connect('ppp_loans.db', check_same_thread=False)
    df.to_sql(name='ppp_loans', con=conn, if_exists='append', index=False)    

# make the table
for link in ppp_csv_url_list:
    create_ppp_loans_table(link)

print('--------------------------------------------')
print('--------------------------------------------')
print(' PPP Loan Data Migration Complete           ')
print('--------------------------------------------')
print('--------------------------------------------')