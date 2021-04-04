from bs4 import BeautifulSoup
import requests
import json
import datetime
import sqlite3
import sys
import os
import shutil

"""
Usage: python3 main.py AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
"""


try:
	AWS_ACCESS_KEY_ID = sys.argv[1]
	AWS_SECRET_ACCESS_KEY = sys.argv[2]
except:
	print("Wrong arguments.")
	sys.exit(1)

# get the absolute path of where this script is
abs_dir = os.path.dirname(os.path.abspath(__file__))

# get date & time
utc_date_now = datetime.datetime.utcnow().strftime("%m/%d/%Y")
last_update_date = ""

# open the file which saves the date of last scraping job
with open(abs_dir + "/last_update_date.txt",'r') as file:
	last_update_date = file.readlines(1)[0]

# if scraping job has been done for the day, end this program
if last_update_date == utc_date_now:
	print("Today's scraping has been done.", datetime.datetime.now().strftime("%H:%M:%S"))
	sys.exit()

# retrive the backup database
rtv_dest = abs_dir + '/game_prices.db'
rtv_src = abs_dir + '/backup_db/game_prices_backup.db'
shutil.copyfile(rtv_src, rtv_dest)

# function to get the lowest price of each stores (only scraping data of base game)
def lowest_price_of_stores(store_prices, store, price):
    if store not in store_prices.keys():
        store_prices[store] = price
    elif price < store_prices[store]:
        store_prices[store] = price

# list of game prices url from gg.deals
URLs =[]

with open(abs_dir + '/game_prices_urls.txt') as url_file:
	URLs = url_file.readlines()
	for i in range(len(URLs)):
		URLs[i] = URLs[i].replace("\n","").replace(" ","")


# create database connection
conn = None
try:
    conn = sqlite3.connect(abs_dir + '/game_prices.db')
    print(sqlite3.version)
except Exception as e:
    print(e)
c = conn.cursor()
print("Start",datetime.datetime.now().strftime("%H:%M:%S"))

for url in URLs:
	try:
		r = requests.get(url)
		soup = BeautifulSoup(r.content, 'html5lib')
		data = json.loads(soup.find('script', type='application/ld+json').text)

	except AttributeError:
		print(url, "page not found.")
		continue
		
	except Exception as e:
		print(e)
		# retr_dest = 'game_prices.db'
		# retr_src = 'backup_db/game_prices_backup.db'
		# shutil.copyfile(retr_src, rete_dest)
		# print("Backup retrived.")
		sys.exit()

	# only scrape prices from stores listed below
	trusted_sellers = ['Steam','Gog.com','Humble Store','Epic Games Store',
	                   'Ubisoft Store','Fanatical', 'WinGameStore','Indie Gala Store',
	                  'Origin', 'Green Man Gaming','Gamesplanet US','2Game US','Gamebillet',
	                  "Microsoft Store","Amazon.com","Battle.net","Voidu","DLGamer.com",
	                  "Allyouplay","Bethesda","GamersGate","Escape From Tarkov"]

	# navigate the scraped json file to get price data
	store_prices = {}
	for offer in data['offers']['offers']:
	    if offer['seller']['name'] in trusted_sellers:
	        lowest_price_of_stores(store_prices, offer['seller']['name'],
	                               offer['price'])

	# generate SQL command for creating new table
	table_name = url.replace("https://gg.deals/","").replace("game","").replace("dlc","").replace("pack","").replace("/","").replace('-','_')
	
	# if the store_prices dictionary is empty
	if not bool(store_prices):
		print("No prices of official stores for", table_name)
		continue

	print(table_name)

	sql_commd = f"""
	CREATE TABLE IF NOT EXISTS {table_name} (
	    date date NOT NULL,
	    time timestamp NOT NULL,
	    PRIMARY KEY(date, time))
	"""
	c.execute(sql_commd)

	# SQL command for creating new column for an online store
	for store in list((store_prices.keys())):
	    try:
	        c.execute(f"ALTER TABLE {table_name} ADD COLUMN \"{store}\" float64")
	    except:
	        pass

	# record scraping date & time
	date_now = "\"" + datetime.datetime.utcnow().strftime("%m/%d/%Y") + "\""
	time_now = "\"" + datetime.datetime.utcnow().strftime("%H:%M:%S") + "\""

	col_str = ', '.join(list(store_prices.keys())).replace(", ","\", \"")
	col_str = "\"" + col_str + "\""

	val_str = str(list(store_prices.values())).replace("[","").replace("]","")

	# insert a new observation
	sql_insert_row = f"""INSERT INTO {table_name} (date, time, {col_str})
	VALUES ({date_now}, {time_now}, {val_str})"""

	c.execute(sql_insert_row)
	conn.commit()

conn.close()
print("Done",datetime.datetime.now().strftime("%H:%M:%S")) # print the message of finishing job

# backup the whole database
backup_src = abs_dir + '/game_prices.db'
backup_dest = abs_dir + '/backup_db/game_prices_backup.db'
shutil.copyfile(backup_src, backup_dest)

# write the date (UTC) into the file to prevent running this program again in the same day
with open(abs_dir + '/last_update_date.txt', 'w') as file:
    file.write(datetime.datetime.utcnow().strftime("%m/%d/%Y"))


# call another python script to trigger AWS Lambda function to send notification through AWS SNS
# Lambda function and SNS are configured on AWS console beforehand
os.system("python3 " + abs_dir.replace(' ', '\ ') + "/lambda_trigger.py " + AWS_ACCESS_KEY_ID + " " + AWS_SECRET_ACCESS_KEY)
