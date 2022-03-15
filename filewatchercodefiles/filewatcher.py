import sys 
import time 
import logging 
import watchdog.events 
import watchdog.observers 
from watchdog.observers import Observer 
from watchdog.events import LoggingEventHandler ,FileSystemEventHandler 
from watchdog.events import FileSystemEventHandler 
from dotenv import dotenv_values,load_dotenv
     
class Handler(watchdog.events.PatternMatchingEventHandler): 
    def __init__(self): 
        watchdog.events.PatternMatchingEventHandler.__init__(self, patterns=['*.csv','*.txt','*.pdf','*.json'], 
                                                             ignore_directories=True, case_sensitive=False)
    
    def on_created(self, event): 
        print("Watchdog received created event - % s." % event.src_path) 


    def on_modified(self, event): 
        print("Watchdog received modified event - % s." % event.src_path) 

        
            

if __name__ == "__main__": 
    config = dotenv_values(".env")
    print(config['LOCALPATH'])
    # Set the format for logging info 
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