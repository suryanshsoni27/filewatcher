import sys 
import time 
import logging
from pymysql import Date 
import watchdog.events 
import watchdog.observers 
from watchdog.observers import Observer 
from watchdog.events import LoggingEventHandler ,FileSystemEventHandler 
from watchdog.events import FileSystemEventHandler 
from dotenv import dotenv_values,load_dotenv 
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists,create_database
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy import create_engine
import os
from postgres import Postgres



class Handler(watchdog.events.PatternMatchingEventHandler): 
    def __init__(self): 
        watchdog.events.PatternMatchingEventHandler.__init__(self, patterns=['*.csv','*.txt','*.pdf','*.json'], 
                                                             ignore_directories=True, case_sensitive=False)
    
    def get_engine(self,url):
        if not database_exists(url):
            create_database(url)
        self.engine = create_engine(url,pool_size=50, echo=False)
        print(self.engine.url)
        return self.engine
    
    def commodityModified(self):
        url = 'sqlite://commodityModified.db'
        self.get_engine(url)
        engine = self.get_engine()
        self.session = sessionmaker(bind = engine)() 
        return self.session
    
    def commodity(self):
        url = 'sqlite://commodity.db'
        self.get_engine(url)
        engine = self.get_engine()
        self.session = sessionmaker(bind = engine)() 
        return self.session
        
       
       
    def get_sessions(self):
        commoditySession = self.commodity()
        commodityModifiedSession = self.commodityModified()
        return commoditySession,commodityModifiedSession
        
    
    #Adds newly created file to the sql database 
    def on_created(self, event):
        cm,mcm = self.get_sessions()
        
        print(event.src_path)
        print("Watchdog received created event - % s." % event.src_path)
        split_tup = os.path.splitext(event.src_path)
        if split_tup[1] == ".csv":
            df = pd.read_csv(event.src_path)
            columns = df.columns
            print(columns)
            df = df.astype({"Date": Date}, errors='raise')
            df = df.astype({"Commodity": str}, errors='raise')
            pathname = event.src_path.split("/")
            path = pathname[len(pathname)-1]
            df = df.astype({"index":Integer})
            columns = df.columns
            for i in columns[2:len(columns)]:
                df[i] = pd.to_numeric(i)
            df['Date'] = pd.to_datetime(df['Date'])
            commoiditydata = Table(columns)
           
            cm.add(df)
            cm.commit()        
           

            
            
    #Adds modified file to sql database
    def on_modified(self, event): 
        cm,mcm = self.get_sessions()
        print("Watchdog received modified event - % s." % event.src_path)
        split_tup = os.path.splitext(event.src_path)
        if split_tup[1] == ".csv":
            
            df = pd.read_csv(event.src_path)
            pathname = event.src_path.split("/")
            path = pathname[len(pathname)-1]
            stagedtable = Table(df.columns)
            mcm.add(df)
            
            #load one row that is modified
            #look into sql merge function to load only the row that has been modified
            #create another table commodity staging table , whenever file is modifed andload it into staging tablen
            #use sql merge to check difference between staging and file 
            
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