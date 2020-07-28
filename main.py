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
URLs =[
'https://gg.deals/game/monster-hunter-world/',
	   "https://gg.deals/dlc/monster-hunter-world-iceborne/",
       'https://gg.deals/game/red-dead-redemption-ii/',
      'https://gg.deals/game/grand-theft-auto-v/',
      'https://gg.deals/game/fallout-4/',
      'https://gg.deals/game/the-witcher-3-wild-hunt/',
      'https://gg.deals/game/cyberpunk-2077/',
      'https://gg.deals/game/kingdom-come-deliverance/',
      'https://gg.deals/game/far-cry-5/',
      'https://gg.deals/game/far-cry-4/',
      'https://gg.deals/game/the-elder-scrolls-v-skyrim-special-edition/',
      "https://gg.deals/game/assassin-s-creed-origins/",
      "https://gg.deals/game/assassin-s-creed-odyssey/",
      "https://gg.deals/game/metal-gear-solid-v-the-phantom-pain/",
      "https://gg.deals/game/tom-clancy-s-ghost-recon-wildlands/",
      "https://gg.deals/game/borderlands-3/",
       "https://gg.deals/game/sekiro-shadows-die-twice/",
       "https://gg.deals/game/cuphead/",
       "https://gg.deals/game/nier-automata/",
       "https://gg.deals/game/divinity-original-sin-2/",
       "https://gg.deals/game/metro-exodus/",
       "https://gg.deals/game/rimworld/",
       "https://gg.deals/game/doom-eternal/",
       "https://gg.deals/game/state-of-decay-2-juggernaut-edition/",
       "https://gg.deals/game/cities-skylines/",
       "https://gg.deals/game/just-cause-3/",
       "https://gg.deals/game/just-cause-4/",
       "https://gg.deals/game/sniper-elite-4/",
       "https://gg.deals/game/watch-dogs-2/",
       "https://gg.deals/game/black-desert-online/",
       "https://gg.deals/game/shadow-of-the-tomb-raider/",
       "https://gg.deals/game/hitman-2/",
       "https://gg.deals/game/terraria/",
       "https://gg.deals/game/grand-theft-auto-san-andreas/",
       "https://gg.deals/game/star-wars-jedi-fallen-order/",
       "https://gg.deals/game/conan-exiles/",
       "https://gg.deals/game/ken-follett-s-the-pillars-of-the-earth/",
       "https://gg.deals/game/final-fantasy-viii-remastered/",
       "https://gg.deals/game/stardew-valley/",
       "https://gg.deals/game/valkyria-chronicles-4/",
       "https://gg.deals/game/thronebreaker-the-witcher-tales/",
       "https://gg.deals/game/mount-blade-ii-bannerlord/",
       "https://gg.deals/game/doom/",
       "https://gg.deals/game/fallout-76/",
       "https://gg.deals/game/ark-survival-evolved/",
       "https://gg.deals/game/new-world/",
       "https://gg.deals/game/death-stranding/",
       "https://gg.deals/game/god-eater-3/",
       "https://gg.deals/game/call-of-duty-black-ops-ii/",
       "https://gg.deals/game/playerunknown-s-battlegrounds/",
       "https://gg.deals/game/halo-the-master-chief-collection/",
       "https://gg.deals/game/hollow-knight/",
       "https://gg.deals/game/ori-and-the-will-of-the-wisps/",
       "https://gg.deals/game/dark-souls-iii/",
       "https://gg.deals/game/call-of-duty---modern-warfare-2019/",
       "https://gg.deals/game/tom-clancy-s-rainbow-six-siege/",
       "https://gg.deals/game/greedfall/",
       "https://gg.deals/game/a-plague-tale-innocence/",
       "https://gg.deals/game/control/",
       "https://gg.deals/game/ashen/",
       "https://gg.deals/game/no-man-s-sky/",
       "https://gg.deals/game/grim-dawn/",
       "https://gg.deals/game/sid-meiers-civilization-vi/",
       "https://gg.deals/game/half-life-alyx/",
       "https://gg.deals/game/tekken-7/",
       "https://gg.deals/game/tom-clancy-s-ghost-recon-breakpoint/",
       "https://gg.deals/game/yakuza-kiwami-2/",
       "https://gg.deals/game/mortal-kombat11/",
       "https://gg.deals/game/total-war-warhammer/",
       "https://gg.deals/game/hunt-showdown/",
       "https://gg.deals/game/detroit-become-human/",
       "https://gg.deals/game/forza-horizon-4/",
       "https://gg.deals/game/frostpunk/",
       "https://gg.deals/game/the-elder-scrolls-online/",
       "https://gg.deals/game/nba-2k20/",
       "https://gg.deals/game/middle-earth-shadow-of-war/",
       "https://gg.deals/game/nioh-complete-edition-complete-edition/",
       "https://gg.deals/game/rise-of-the-tomb-raider/",
       "https://gg.deals/game/dragon-ball-fighterz/",
       "https://gg.deals/game/devil-may-cry-5/",
       "https://gg.deals/game/darksiders-genesis/",
       "https://gg.deals/game/battlefield-v/",
       "https://gg.deals/game/dragon-quest-xi-echoes-of-an-elusive-age/",
       "https://gg.deals/game/soulcalibur-vi/",
       "https://gg.deals/game/tom-clancys-splinter-cell-blacklist/",
       "https://gg.deals/game/assassins-creed-valhalla/",
       "https://gg.deals/game/darksiders-iii/",
       "https://gg.deals/game/rage-2/",
       "https://gg.deals/pack/payday-2-legacy-collection/",
       "https://gg.deals/game/anthem/",
       "https://gg.deals/game/ultimate-chicken-horse/",
       "https://gg.deals/game/they-are-billions/",
       "https://gg.deals/game/battlefield-1/",
       "https://gg.deals/game/code-vein/",
       "https://gg.deals/game/max-payne-3/",
       "https://gg.deals/game/wolfenstein-ii-the-new-colossus/",
       "https://gg.deals/game/prey/",
       "https://gg.deals/game/shadow-warrior-2/",
       "https://gg.deals/game/wolfenstein-the-new-order/",
       "https://gg.deals/game/sword-art-online-fatal-bullet/",
       "https://gg.deals/game/sword-art-online-hollow-realization-deluxe-edition/",
       "https://gg.deals/game/sword-art-online-alicization-lycoris/",
       "https://gg.deals/game/gears-tactics/",
       "https://gg.deals/game/gears-5/",
       "https://gg.deals/game/resident-evil-3/",
       "https://gg.deals/game/ace-combat-7-skies-unknown/",
       "https://gg.deals/game/xcom-2/",
       "https://gg.deals/game/resident-evil-2-biohazard-re-2/",
       "https://gg.deals/game/dragons-dogma-dark-arisen/",
       "https://gg.deals/game/attack-on-titan-2-a-o-t-2/",
       "https://gg.deals/game/warriors-orochi-4/",
       "https://gg.deals/game/dynasty-warriors-9/",
       "https://gg.deals/game/dynasty-warriors-8-empires/",
       "https://gg.deals/game/dynasty-warriors-8-xtreme-legends-complete-edition/",
       "https://gg.deals/game/one-piece-pirate-warriors-4/",
       "https://gg.deals/game/sid-meier-s-civilization-v/",
       "https://gg.deals/game/total-war-three-kingdoms/",
       "https://gg.deals/game/romance-of-the-three-kingdoms-xiv/",
       "https://gg.deals/game/samurai-warriors-spirit-of-sanada/",
       "https://gg.deals/game/toukiden-2/",
       "https://gg.deals/game/dead-or-alive-6/",
       "https://gg.deals/game/for-honor/",
       "https://gg.deals/game/naruto-shippuden-ultimate-ninja-storm-4/",
       "https://gg.deals/game/jump-force/",
       "https://gg.deals/game/naruto-to-boruto-shinobi-striker/",
       "https://gg.deals/game/mafia-definitive-edition/",
       "https://gg.deals/game/mafia-iii/",
       "https://gg.deals/game/devil-may-cry-4-special-edition/",
       "https://gg.deals/game/devil-may-cry-hd-collection/",
       "https://gg.deals/game/pillars-of-eternity-ii-deadfire/",
       "https://gg.deals/game/bloodstained-ritual-of-the-night/",
       "https://gg.deals/game/tropico-6/",
       "https://gg.deals/game/tales-of-berseria/",
       "https://gg.deals/game/final-fantasy-ix/",
       "https://gg.deals/game/the-outer-worlds/",
       "https://gg.deals/game/ys-viii-lacrimosa-of-dana-viii-lacrimosa-of-dana/",
       "https://gg.deals/game/insurgency-sandstorm/",
       "https://gg.deals/game/marvels-avengers/",
       "https://gg.deals/game/sea-of-thieves/",
       "https://gg.deals/game/rust/",
       "https://gg.deals/game/far-cry-new-dawn/",
       #"https://gg.deals/game/7-days-to-die/",
       "https://gg.deals/game/subnautica/",
       "https://gg.deals/game/far-cry-primal/",
       "https://gg.deals/game/tom-clancy-s-the-division/",
       "https://gg.deals/game/tom-clancys-the-division-2/",
       "https://gg.deals/dlc/the-division-2-warlords-of-new-york-expansion/",
       "https://gg.deals/game/moving-out/",
       "https://gg.deals/game/vampyr/",
       "https://gg.deals/pack/borderlands-the-handsome-bundle/",
       "https://gg.deals/game/disco-elysium/",
       "https://gg.deals/game/planet-zoo/",
       "https://gg.deals/game/planet-coaster/",
       "https://gg.deals/game/the-forest/",
       "https://gg.deals/game/euro-truck-simulator-2/",
       "https://gg.deals/game/american-truck-simulator/",
       "https://gg.deals/game/project-cars-2/",
       "https://gg.deals/game/house-flipper/",
       "https://gg.deals/game/sleeping-dogs-definitive-edition/",
       "https://gg.deals/game/torchlight-ii/",
       "https://gg.deals/game/the-incredible-adventures-of-van-helsing/",
       "https://gg.deals/game/warhammer-vermintide-2/",
       "https://gg.deals/game/titan-quest-anniversary-edition/",
       "https://gg.deals/game/minecraft-dungeons/",
       "https://gg.deals/game/arma-3/",
       "https://gg.deals/game/ori-and-the-blind-forest-definitive-edition/",
       "https://gg.deals/game/need-for-speed-hot-pursuit/",
       "https://gg.deals/game/need-for-speed-shift/",
       "https://gg.deals/game/need-for-speed-heat/",
       "https://gg.deals/game/need-for-speed-payback/",
       "https://gg.deals/game/need-for-speed/",
       "https://gg.deals/game/need-for-speed-rivals/",
       "https://gg.deals/game/need-for-speed-most-wanted/",
       "https://gg.deals/game/need-for-speed-the-run/",
       "https://gg.deals/pack/fallout-4-game-of-the-year-edition/",
       "https://gg.deals/game/persona-4-golden/",
       "https://gg.deals/game/torchlight-iii/",
       "https://gg.deals/game/horizon-zero-dawn-complete-edition/",
       "https://gg.deals/game/nba-2k21/",
       "https://gg.deals/game/ea-sports-fifa-21/",
       "https://gg.deals/game/madden-nfl-21/",
       "https://gg.deals/game/ni-no-kuni-wrath-of-the-white-witch-remastered/",
       "https://gg.deals/game/f1-2020/",
       "https://gg.deals/game/crusader-kings-iii/",
       "https://gg.deals/game/my-time-at-portia/",
       "https://gg.deals/game/wasteland-3/",
       "https://gg.deals/game/windbound/",
       "https://gg.deals/game/othercide/",
       "https://gg.deals/game/dead-space-3/",
       "https://gg.deals/pack/star-wars-battlefront-ii-celebration-edition/",
       "https://gg.deals/game/trials-of-mana/",
       "https://gg.deals/game/fairy-tail/",
       "https://gg.deals/game/skater-xl/",
       "https://gg.deals/game/fall-guys-ultimate-knockout/",
       "https://gg.deals/game/tell-me-why/",
       "https://gg.deals/game/carrion/",
       "https://gg.deals/game/roki/",
       "https://gg.deals/game/beyond-a-steel-sky/",
       "https://gg.deals/game/ooblets/",
       "https://gg.deals/game/neon-abyss/",
       "https://gg.deals/game/little-witch-nobeta/",
       "https://gg.deals/game/spongebob-squarepants-battle-for-bikini-bottom-rehydrated/",
       "https://gg.deals/game/p-a-m-e-l-a/",
       "https://gg.deals/game/immortal-realms-vampire-wars/",
       "https://gg.deals/game/iron-harvest/",
       "https://gg.deals/game/serious-sam-4-planet-badass/",
       "https://gg.deals/game/resident-evil-7-biohazard/",
       "https://gg.deals/game/star-wars-squadrons/",
       "https://gg.deals/game/yakuza-like-a-dragon/",
       "https://gg.deals/game/dirt-5/",
       "https://gg.deals/game/xiii-remake/",
       "https://gg.deals/game/desperados-iii/",
       "https://gg.deals/game/a-way-out/",
       "https://gg.deals/game/disintegration/",
       "https://gg.deals/pack/dragon-age-inquisition-game-of-the-year-edition/",
       "https://gg.deals/game/tales-of-vesperia-definitive-edition/",
       #"https://gg.deals/game/ghostrunner/",
      ]

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