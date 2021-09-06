import mysql.connector
from mysql.connector import Error
import re
import datetime
import pandas as pd
from sqlalchemy import create_engine
try:
###This is the try block within which a connection is made between python and database using ###
    
    my_conn = create_engine("mysql+mysqldb://root:Nihar@123@localhost/patients") # A connection is made to database using create_engine here an engine object is created
                                
                                
    df = pd.read_csv('C:/Users/Nihar/Onedrive/Desktop/data.txt',sep="|",header=None,skiprows=1)# database file is read using read_csv function into dataframe
  
    df=df.drop([0,1], axis = 1)#dropped first two columns as tehry were unnecessaruy for our database
    df.columns=["Customer_name", "Customer_id", "Customer_open_date", "Last_consulted_date","vaccination_type",
    "doctor_consulted","state","country","date_of_birth","active_customer"]# dataframe is given appropriate headers as per our database
    df["date_of_birth"]=df["date_of_birth"].apply(lambda x: str(x)[3:7]+str(x)[1:3]+str(x)[0:1])# format of date is changed from yyyy-mm-dd format to dd-mm-yyyy format
    
    
    # different dataframes are created for different countries and the data is stored in different dataframes as per different countries
    usa = df[df["country"] == 'USA']
    ind = df[df["country"] =='IND']
    au = df[df ["country"] == 'AU']
    phil = df[df["country"]=='PHIL']
    nyc = df[df["country"] == 'NYC']
    
    # the whole data frame is pushed into database 
    usa.to_sql(con=my_conn, name='usa', if_exists='append')
    ind.to_sql(con=my_conn, name='ind', if_exists='append')
    au.to_sql(con=my_conn, name='au', if_exists='append')
    phil.to_sql(con=my_conn, name='phil', if_exists='append')
    nyc.to_sql(con=my_conn, name='nyc', if_exists='append')
    
except Error as e:
    print("Error while connecting to MySQL", e)

    
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
