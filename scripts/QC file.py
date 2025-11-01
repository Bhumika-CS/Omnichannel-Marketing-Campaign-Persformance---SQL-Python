#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 29 19:24:27 2025

@author: Bhumika
"""

def QC():
    import pandas as pd
    import psycopg2

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",        # or your DB server address
        database="postgres",# replace with your DB name
        user="postgres",    # your PostgreSQL username
        password="Black@12",# your PostgreSQL password
        port="5432"              # default PostgreSQL port
    )

    # Write your SQL query
    query1 = "SELECT * FROM public.\"Consolidated_Campaign_Data\";"
    query2 = "SELECT * FROM public.\"Consolidated_Campaign_Data_Backup\";"

    #Load query results into a pandas DataFrame
    df = pd.read_sql_query(query1, conn)
    df_backup=pd.read_sql_query(query2, conn)

    # Close the connection
    conn.close()

    # Read the CSV file
    #df = pd.read_csv("/Users/Bhumika/_Consolidated_Campaign_Data__202511011918.csv")
    #df_backup = pd.read_csv("/Users/Bhumika/_Consolidated_Campaign_Data__Backup_202511011918.csv")

    #source 
    sources=['Third party', 'Internal']
    if df['source'].isin(sources).all():
        print("source column is verified")         
    else:
        print("Check source column")    

    #vendor
    vendors=['Vendor 1', 'Vendor 2','internal']
    if df['vendor'].isin(vendors).all():
        print("vendor column is verified")         
    else:
        print("Check vendor column")  
            
    #channel_name
    channels=['Display', 'Third party email','Email']
    if df['channel_name'].isin(channels).all():
        print("channel_name is verified")         
    else:
        print("Check channel_name column")    

    #campaign_name
    campaigns = ['ABC drug launch', 'ABC med awareness']
    mask = df['campaign_name'].isin(campaigns) | df['campaign_name'].isna()
    if mask.all()==True:
        print("campaign_name is verified")
    else:
        print("Check campaign_name column")
        
    #subject_line
    campaigns = ['Why choose ABC?', 'Latest research on ABC']
    mask = df['subject_line'].isin(campaigns) | df['subject_line'].isna()
    if mask.all()==True:
        print("subject_line is verified")
    else:
        print("Check subject_line column")

    #npi
    invalid_npi =df.loc[df['npi'].astype(str).str.len() != 10,'npi']
    if invalid_npi.empty:
        print("npi is verified")
    else:
        print("check npi length - "+invalid_npi.astype(str))
        
    if df['npi'].isna().any():
        print("check null value in npi column")
    else:
        print("npi column is verified")
        
    #mdm_id
    invalid_mdm=df.loc[df['mdm_id'].astype(str).str.len()!=8,'mdm_id']
    if invalid_mdm.empty:
        print("mdm_id is verified")
    else:
        print("Check mdm_id column - "+invalid_mdm.astype(str))

    #delivered_datetime
    if df['delivered_datetime'].isnull().any():
        print("check delivered_datetime column")
    else:
        print("delivered_datetime is verified") 

    print(str(min(df['delivered_datetime']))+"is the minimum delivered_datetime")
    print(str(max(df['delivered_datetime']))+"is the maximum delivered_datetime")

    #no_of_sends
    if df['no_of_sends'].any()<=0:
        print("check no_of_sends column")
    else:
        print("no_of_sends is verified")
        
    #primary_eng_datetime
    if (df['primary_eng_datetime'] < df['delivered_datetime']).any():
        print("check primary_eng_datetime column")
    else:
        print("primary_eng_datetime is verified")


    # Convert to datetime safely
    df['primary_eng_datetime'] = pd.to_datetime(df['primary_eng_datetime'], errors='coerce')
    # Drop nulls (optional, just for min calc)
    min_datetime = df['primary_eng_datetime'].min()
    max_datetime = df['primary_eng_datetime'].max()
    print(str(min_datetime) + " is the minimum primary_eng_datetime")
    print(str(max_datetime) + " is the maximum primary_eng_datetime")

    #no_of_prim_eng
    if df['no_of_prim_eng'].any()<0:
        print("check no_of_prim_eng column")
    else:
        print("no_of_prim_eng is verified") 

    #sencondary_eng_datetime---

    # Ensure datetime columns are proper datetimes
    df[['sencondary_eng_datetime', 'delivered_datetime', 'primary_eng_datetime']] = (
        df[['sencondary_eng_datetime', 'delivered_datetime', 'primary_eng_datetime']]
        .apply(pd.to_datetime, errors='coerce')
    )

    # Directly in the if condition
    if ((df['sencondary_eng_datetime'].isna()) | 
        ((df['sencondary_eng_datetime'] >= df['delivered_datetime']) & 
         (df['sencondary_eng_datetime'] >= df['primary_eng_datetime']))).all():
        print("secondary_eng_datetime is verified")
    else:
        print("Check secondary_eng_datetime column")
        
    #no_of_sec_eng
    if df['no_of_sec_eng'].any()<0:
        print("check no_of_sec_eng column")
    else:
        print("no_of_sec_eng is verified")

    #acad_comm
    types1=['Academic','Community']
    if df['acad_comm'].isin(types1).all():
        print('acad_comm is verified')
    else:
        print('check acad_comm column')

    #teaching_hospital
    types2=['Yes','No']
    if df['teaching_hospital'].isin(types2).all():
        print('teaching_hospital is verified')
    else:
        print('check teaching_hospital column')

    #region
    types3=['West', 'North', 'South', 'Central', 'East']
    if df['region'].isin(types3).all():
        print('region is verified')
    else:
        print('check region column')

    #prescriber
    types2=['Yes','No']
    if df['prescriber'].isin(types2).all():
        print('prescriber is verified')
    else:
        print('check prescriber column')

    #rowcount
    print("Current row count for each column:\n", df.count())
    print("Previous row count for each column:\n", df_backup.count())

QC()    


















