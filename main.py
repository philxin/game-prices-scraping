from bs4 import BeautifulSoup
import requests
import json
import datetime
import sqlite3
import sys
import os
import shutil

abs_dir = os.path.dirname(os.path.abspath(__file__))

utc_date_now = datetime.datetime.utcnow().strftime("%m/%d/%Y")
last_update_date = ""

with open(abs_dir + "/last_update_date.txt",'r') as file:
	last_update_date = file.readlines(1)[0]

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

conn = None
try:
    conn = sqlite3.connect(abs_dir + '/game_prices.db')
    print(sqlite3.version)
except Error as e:
    print(e)
c = conn.cursor()
print("Start",datetime.datetime.now().strftime("%H:%M:%S"))

for url in URLs:
	try:
		r = requests.get(url)
		soup = BeautifulSoup(r.content, 'html5lib')
	except Error as e:
		print(e)
		# retr_dest = 'game_prices.db'
		# retr_src = 'backup_db/game_prices_backup.db'
		# shutil.copyfile(retr_src, rete_dest)
		# print("Backup retrived.")
		sys.exit()

	data = json.loads(soup.find('script', type='application/ld+json').text)


	trusted_sellers = ['Steam','Gog.com','Humble Store','Epic Games Store',
	                   'Ubisoft Store','Fanatical', 'WinGameStore','Indie Gala Store',
	                  'Origin', 'Green Man Gaming','Gamesplanet US','2Game US','Gamebillet',
	                  "Microsoft Store","Amazon.com","Battle.net","Voidu","DLGamer.com",
	                  "Allyouplay","Bethesda","GamersGate"]

	store_prices = {}
	for offer in data['offers']['offers']:
	    if offer['seller']['name'] in trusted_sellers:
	        lowest_price_of_stores(store_prices, offer['seller']['name'],
	                               offer['price'])

	table_name = url.replace("https://gg.deals/","").replace("game","").replace("dlc","").replace("pack","").replace("/","").replace('-','_')
	print(table_name)
	sql_commd = f"""
	CREATE TABLE IF NOT EXISTS {table_name} (
	    date date NOT NULL,
	    time timestamp NOT NULL,
	    PRIMARY KEY(date, time))
	"""
	c.execute(sql_commd)

	for store in list((store_prices.keys())):
	    try:
	        c.execute(f"ALTER TABLE {table_name} ADD COLUMN \"{store}\" float64")
	    except:
	        pass

	date_now = "\"" + datetime.datetime.utcnow().strftime("%m/%d/%Y") + "\""
	time_now = "\"" + datetime.datetime.utcnow().strftime("%H:%M:%S") + "\""
	#print(date_now, time_now)
	col_str = ', '.join(list(store_prices.keys())).replace(", ","\", \"")
	col_str = "\"" + col_str + "\""
	#print(col_str)
	val_str = str(list(store_prices.values())).replace("[","").replace("]","")
	#print(val_str)

	sql_insert_row = f"""INSERT INTO {table_name} (date, time, {col_str})
	VALUES ({date_now}, {time_now}, {val_str})"""

	c.execute(sql_insert_row)
	conn.commit()

conn.close()
print("Done",datetime.datetime.now().strftime("%H:%M:%S"))

# backup the whole database
backup_src = abs_dir + '/game_prices.db'
backup_dest = abs_dir + '/backup_db/game_prices_backup.db'
shutil.copyfile(backup_src, backup_dest)


with open(abs_dir + '/last_update_date.txt', 'w') as file:
    file.write(datetime.datetime.utcnow().strftime("%m/%d/%Y"))