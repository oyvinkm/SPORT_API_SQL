import time
import pyodbc

from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

from http_request import proccess_post_api_data

# Configuration for your database
DATABASE_CONFIG = {
    'driver': 'ODBC Driver 18 for SQL Server',
    'server': 'localhost',
    'port' : '1433',
    'database': 'sportScoreDB',
    'uid': 'sa',
    'pwd': 'reallyStrongPwd123'
}


def get_db_connection():
    conn = pyodbc.connect(f"DRIVER={DATABASE_CONFIG['driver']};\
                SERVER={DATABASE_CONFIG['server']}; \
                PORT={DATABASE_CONFIG['port']}; \
                DATABASE={DATABASE_CONFIG['database']}; \
                UID={DATABASE_CONFIG['uid']}; \
                PWD={DATABASE_CONFIG['pwd']}; \
                TrustServerCertificate=yes")
    return conn

def refresh_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    conn.commit()
    # For example, re-fetching data from an API and updating the database
    print("Database refreshed at", time.strftime("%Y-%m-%d %H:%M:%S"))
    conn.close()

def refresh_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Start a transaction
        cursor.execute("BEGIN TRANSACTION;")

        # Fetching API data nad updating the database
        proccess_post_api_data(cursor)
        print("Refreshing database...")
        # Delete old entries older than 1 day
        cursor.execute("DELETE FROM Games WHERE startTimestamp < DATEADD(day, -1, GETDATE());")
        print('Old games deleted...')
        # Commit the transaction if all operations are successful
        cursor.execute("COMMIT;")

        print("Database refreshed at", datetime.now())
    except Exception as e:
        # Rollback the transaction in case of error
        cursor.execute("ROLLBACK;")
        print("Error refreshing database:", e)
    finally:
        conn.close()

if __name__ == '__main__':
  print('Scheduler started...')
  scheduler = BackgroundScheduler()
  scheduler.add_job(refresh_database, 'interval', minutes=.5)  # Adjust interval as needed
  scheduler.start()

  try:
      while True:
          time.sleep(2)
  except (KeyboardInterrupt, SystemExit):
      scheduler.shutdown()