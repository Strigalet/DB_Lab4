import csv
import os
import re
import time
import pymongo

row_nums = [1, 18, 19, 20, 21, 29, 30, 31, 39, 40, 41, 49, 50, 51, 59, 60, 61, 69, 70, 71, 79, 80, 81, 88, 89, 91, 98, 99, 101, 108, 109, 111, 118, 119, 121]
columns = ['inserted', 'year', '_id', 'Birth', 'SEXTYPENAME', 'REGNAME', 'AREANAME', 'TERNAME', 'REGTYPENAME', 'TerTypeName', 'ClassProfileNAME', 'ClassLangName', 'EONAME', 'EOTYPENAME', 'EORegName', 'EOAreaName', 'EOTerName', 'EOParent', 'UkrTest', 'UkrTestStatus', 'UkrBall100', 'UkrBall12', 'UkrBall', 'UkrAdaptScale', 'UkrPTName', 'UkrPTRegName', 'UkrPTAreaName', 'UkrPTTerName', 'histTest', 'HistLang', 'histTestStatus', 'histBall100', 'histBall12', 'histBall', 'histPTName', 'histPTRegName', 'histPTAreaName', 'histPTTerName', 'mathTest', 'mathLang', 'mathTestStatus', 'mathBall100', 'mathBall12', 'mathBall', 'mathPTName', 'mathPTRegName', 'mathPTAreaName', 'mathPTTerName', 'physTest', 'physLang', 'physTestStatus', 'physBall100', 'physBall12', 'physBall', 'physPTName', 'physPTRegName', 'physPTAreaName', 'physPTTerName', 'chemTest', 'chemLang', 'chemTestStatus', 'chemBall100', 'chemBall12', 'chemBall', 'chemPTName', 'chemPTRegName', 'chemPTAreaName', 'chemPTTerName', 'bioTest', 'bioLang', 'bioTestStatus', 'bioBall100', 'bioBall12', 'bioBall', 'bioPTName', 'bioPTRegName', 'bioPTAreaName', 'bioPTTerName', 'geoTest', 'geoLang', 'geoTestStatus', 'geoBall100', 'geoBall12', 'geoBall', 'geoPTName', 'geoPTRegName', 'geoPTAreaName', 'geoPTTerName', 'engTest', 'engTestStatus', 'engBall100', 'engBall12', 'engDPALevel', 'engBall', 'engPTName', 'engPTRegName', 'engPTAreaName', 'engPTTerName', 'fraTest', 'fraTestStatus', 'fraBall100', 'fraBall12', 'fraDPALevel', 'fraBall', 'fraPTName', 'fraPTRegName', 'fraPTAreaName', 'fraPTTerName', 'deuTest', 'deuTestStatus', 'deuBall100', 'deuBall12', 'deuDPALevel', 'deuBall', 'deuPTName', 'deuPTRegName', 'deuPTAreaName', 'deuPTTerName', 'spaTest', 'spaTestStatus', 'spaBall100', 'spaBall12', 'spaDPALevel', 'spaBall', 'spaPTName', 'spaPTRegName', 'spaPTAreaName', 'spaPTTerName']
batch_size = 800


connection = pymongo.MongoClient('mongodb://localhost:27017')

dbase = connection.zno_database
collection = dbase.zno_collection

def pymongo_populate():

	start = time.time()
	for title in os.listdir('data'):
		year = re.findall(r'Odata(\d{4})File.csv', title)
		if year:
			with open(os.path.join('data', title), encoding='cp1251') as csvfile:

				csv_reader = csv.reader(csvfile, delimiter=';')
				next(csv_reader)
				count = 0
				setting = list()

				# Skipping inserted data
				skip = collection.find_one({'year' : int(year[0])}, sort=[('inserted', -1)])
				if skip:
					if 'inserted' not in skip:
						print('File skipped')
						continue
					for i in range(skip['inserted'] + 1):
						next(csv_reader)
						count += 1
				
				
				for row in csv_reader:
					# Validating data
					for i in range(len(row)):
						if row[i] == 'null':
							row[i] = None
						else:
							if i in row_nums:
								row[i] = row[i].replace(',', '.')
								row[i] = float(row[i])

					
					# Inserting data
					setting.append(dict(zip(columns,[count]+[int(year[0])]+row)))
					count += 1
					if not count % batch_size:
						# Inserting batch of data
						collection.insert_many(setting)
						setting = list()
						
				# Unset the auxiliary column		
				if setting:
					collection.insert_many(setting)
					collection.update_many({}, {'$unset': {'inserted': 1}})
					setting = list()
					

	end = time.time() - start
	result = open("time.txt","w")
	result.write(f'Done. Execution time - {end} seconds\n')


	result.close()
	

def aggregation_select():


	aggregation = collection.aggregate([
			{
				'$match': {
					'UkrTestStatus': 'Зараховано'
				}
			},
			{
				'$group': {
					'_id': {'region': '$REGNAME', 'year': '$year'},
					'avgBall': { '$avg': '$UkrBall' },
					'avgBall100': { '$avg': '$UkrBall100'},
					'avgBall12': {'$avg': '$UkrBall12'}
				}
			}
		])

	with open('result.csv', 'w', encoding="utf-8") as result:
			writer = csv.writer(result)
			writer.writerow(['region', 'year', 'avgBall','avgBall100','avgBall12'])

			for key in aggregation:
				seq = [key['_id']['region'],key['_id']['year'],key['avgBall'],key['avgBall100'],key['avgBall12']]
				writer.writerow(seq)

pymongo_populate()
aggregation_select()
#dbase.drop_collection(collection)