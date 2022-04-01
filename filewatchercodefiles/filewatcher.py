from datetime import date
import sys 
import time 
import logging
import watchdog.events 
import watchdog.observers 
from watchdog.observers import Observer 
from watchdog.events import LoggingEventHandler ,FileSystemEventHandler 
from watchdog.events import FileSystemEventHandler 
from dotenv import dotenv_values,load_dotenv 
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy import create_engine
import os
from postgres import Postgres



class Handler(watchdog.events.PatternMatchingEventHandler): 
    def __init__(self): 
        watchdog.events.PatternMatchingEventHandler.__init__(self, patterns=['*.csv','*.txt','*.pdf','*.json'], 
                                                             ignore_directories=True, case_sensitive=False)
    
    def get_engine(self):
        self.engine = create_engine('sqlite:///commodity.db', echo=False)
        self.session = self.engine.connect()
        return self.engine,self.session
    
    
    #how to add a model 
    
    
    def commodityModifiedUpload(self,df,path):
        sqlConnection,engine=  self.get_engine()
        sqlite_table = "CommodityModified_" + path
        df.to_sql(sqlite_table, sqlConnection, if_exists='fail') 
        
        
    
    def commodityUpload(self,df,path):
        sqlConnection,engine=  self.get_engine()
        sqlite_table = "Commodity_" + path
        df.to_sql(sqlite_table, sqlConnection, if_exists='fail')  

 
    #Adds newly created file to the sql database 
    def on_created(self, event):
        print(event.src_path)
        print("Watchdog received created event - % s." % event.src_path)
        split_tup = os.path.splitext(event.src_path)
        if split_tup[1] == ".csv":
            df = pd.read_csv(event.src_path)
            columns = df.columns
            print(columns)
            pathname = event.src_path.split("/")
            path = pathname[len(pathname)-1]          
            columns = df.columns
            for i in columns[2:len(columns)]:
                df[i] = df[i].str.rstrip('%').astype('float')
            df['Date'] = pd.to_datetime(df['Date'])
            self.commodityUpload(df,path) 
           

    
            
    #Adds modified file to sql database
    def on_modified(self, event): 
        print("Watchdog received modified event - % s." % event.src_path)
        split_tup = os.path.splitext(event.src_path)
        if split_tup[1] == ".csv":   
            
            # df = pd.DataFrame(result)
            # df = pd.read_csv(event.src_path)
            # columns = df.columns
            # pathname = event.src_path.split("/")
            # path = pathname[len(pathname)-1]  
            # columns = df.columns
            # result = self.engine.connect().execute(("SELECT * FROM {pathname}".format(pathname=pathname)))
            # result = ([ dict(zip(i.keys(), i.values())) for i in result ])    
            # df = pd.DataFrame(result)
  
            # changed_df = df.merge(df, indicator=True, how='outer')
            # print(changed_df[changed_df['_merge'] == 'right_only'])
            # for i in columns[2:len(columns)]:
            #     df[i] = df[i].str.rstrip('%').astype('float')
                
            # df['Date'] = pd.to_datetime(df['Date'])
            # self.commodityModifiedUpload(df,path) 
            
            df = pd.read_csv(event.src_path)
            columns = df.columns
            pathname = event.src_path.split("/")
            path = pathname[len(pathname)-1]  
            columns = df.columns
            
            for i in columns[2:len(columns)]:
                df[i] = df[i].str.rstrip('%').astype('float')
                
            df['Date'] = pd.to_datetime(df['Date'])
            self.commodityModifiedUpload(df,path) 
            
            
            # #load one row that is modified
            # #look into sql merge function to load only the row that has been modified
            # #create another table commodity staging table , whenever file is modifed andload it into staging tablen
            # #use sql merge to check difference between staging and file 
            
    def on_deleted(self, event):
        print(event.src_path, event.event_type)
        
        

        
            

if __name__ == "__main__": 
    config = dotenv_values(".env")
    print(config['LOCALPATH'])
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(message)s', 
                        datefmt='%Y-%m-%d %H:%M:%S') 

    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    event_handler1 = LoggingEventHandler() 
    observer = Observer() 
    observer.schedule(event_handler1, path, recursive=True) 
    observer.start() 
    src_path = config['LOCALPATH']
    event_handler = Handler() 
    observer2 = watchdog.observers.Observer() 
    observer2.schedule(event_handler, path=src_path, recursive=True) 
    observer2.start() 
    
    try: 
        while True: 
            time.sleep(1) 
    except KeyboardInterrupt: 
        observer.stop() 
    observer.join() 