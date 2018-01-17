import psycopg2
import geopandas as gpd
from id3 import Id3Estimator
from id3 import export_graphviz
from sklearn.model_selection import train_test_split
from sklearn import tree
import pickle
import time

reliefThreshold = 50

conn = psycopg2.connect(database = "postgis_24_sample", user = 'postgres', password='dd', host='localhost', port = 5432)
cursor = conn.cursor()
print ("Connected to database!\n")

#tracts - 2166
# dane przestrzenne
# nyc_streets: oneway, type - 19091
# nyc_subway_stations - 491
# #

class DBData:
    def __init__(self, _idx, _value, _predicates):
        self.idx = _idx
        self.value = _value
        self.predicates = _predicates

spatialSelects = []

spatialSelects.append("SELECT count(*)>0 FROM nyc_subway_stations WHERE ST_Contains(ST_GeomFromText((%s)), geom);")
cursor.execute("SELECT DISTINCT type from nyc_streets GROUP BY type;")
streets_types = list(cursor.fetchall())

for streets_type in streets_types:
    spatialSelects.append("SELECT count(*)>0 FROM nyc_streets AS streets WHERE streets.type = '" + streets_type[0] + "' AND ST_Intersects(ST_GeomFromText((%s)), streets.geom);")


def getDataFromDB (census_blocks, weights=[]):
    cursor.execute("SELECT DISTINCT type from nyc_streets GROUP BY type;")
    streets_types = cursor.fetchall()
    data= []
    for row in census_blocks:
        #minimal bounding box dla segmentu
        cursor.execute("SELECT ST_ASText(ST_Envelope(geom)) FROM nyc_census_tracts WHERE tractid=(%s)",  (row[0],))
        mbb = cursor.fetchone()[0]

        predicates = []

        #wagi z RELIEF
        if(len(weights)==0):
            for select in spatialSelects:
                cursor.execute(select, (mbb, ))
                predicates.append(cursor.fetchone()[0])

        else:
            for i in range(len(weights)):
                if(weights[i] > reliefThreshold):
                    cursor.execute(spatialSelects[i], (mbb, ))
                    predicates.append(cursor.fetchone()[0])

        #cursor.execute("SELECT count(subway.geom) FROM nyc_census_tracts AS census JOIN nyc_subway_stations AS subway  ON ST_Contains(census.geom, subway.geom) AND  census.tractid = (%s);", (row[0],)) 
        cursor.execute("SELECT count(*)>0 FROM nyc_subway_stations WHERE ST_Contains(ST_GeomFromText((%s)), geom);", (mbb,)) 
        selectRes = cursor.fetchone()
        
        #predicates.append(selectRes[0])

        for streets_type in streets_types:
            cursor.execute("SELECT count(*)>0 FROM nyc_streets AS streets WHERE streets.type = (%s) AND ST_Intersects(ST_GeomFromText((%s)), streets.geom);", (streets_type,mbb,))
            #predicates.append(cursor.fetchone()[0])
            
        
        value = row[1]
        idx = row[0]

        data.append(DBData(idx, value, predicates))

        if(len(data)%300 == 0):
            print("Building :", 100*len(data)/len(census_blocks), "%")
    return data

def reliefAlg(data):
    weights = [0 for i in range(len(data[0].predicates)-1)]
    for i in range (len(data)-1):
        cursor.execute("SELECT tractid, ST_ASText((geom)) FROM nyc_census_tracts WHERE tractid=(%s)", (data[i].idx, ))
        ddata = cursor.fetchone()
        tractid = ddata[0]
        geom = ddata[1]
        cursor.execute("SELECT tractid FROM nyc_census_tracts WHERE tractid != (%s) ORDER BY geom <-> (%s)", (tractid, geom, ))
        closestGeoms = cursor.fetchall()

        nearestHit = False
        nearestMiss = False
        
        #znajdź indeks w danych najbliższego
        #while (not nearestHit and not nearestMiss):
        for row in closestGeoms:
            idxs = [x.idx for x in data]
            if(row[0] not in idxs):
                continue
            closestIdx = idxs.index(row[0])
            #closestIdx = ids.index(cursor.fetchone()[0])
            #znaleziono nearestHit
            if(data[closestIdx].value == data[i].value):
                nearestHit = closestIdx
            #znaleziono nearestMiss
            else:
                nearestMiss = closestIdx
            if(nearestHit and nearestMiss):
                break
        
        #uaktualnij wagi dla każdej cechy
        for weight in range(len(weights)-1):
            if(data[i].predicates[weight] == data[nearestHit].predicates[weight]):
                weights[weight] += 1
            else:
                weights[weight] -= 1
            if(data[i].predicates[weight] == data[nearestMiss].predicates[weight]):
                weights[weight] -= 1
            else:
                weights[weight] += 1

        if(i%300 == 0):
            print("Relief:", 100*i/len(data), "%")

    return weights

def getNonSpatialPredicates(inData):
    #outData = []
    for tract in inData:
        cursor.execute("SELECT popn_total, popn_white, popn_black, popn_nativ, popn_asian, popn_other FROM  nyc_census_tracts WHERE tractid = (%s);", (tract.idx,))
        tractData = list(cursor.fetchone())
        if tractData[0] != 0:
            for i in range(1, 6):
                tractData[i] = tractData[i]/tractData[0]
            
        cursor.execute("SELECT transit_total, transit_private, transit_public, transit_walk, transit_walk, transit_other, transit_none, transit_time_mins, family_count, family_income_median, family_income_mean, family_income_aggregate, edu_total, edu_no_highschool_dipl, edu_highschool_dipl, edu_college_dipl, edu_graduate_dipl FROM nyc_census_sociodata WHERE tractid = (%s);", (tract.idx,))
        socioResult = (cursor.fetchone())
        if socioResult is not None:
            socioData = list(socioResult)
            
            if socioData[0] != 0:
                for i in range(1, 7):
                    socioData[i] = socioData[i]/socioData[0]
            #if socioData[8] == 0:
            #    for i in range(9, 7):

        else:
            socioData = [0 for i in range(17)]
        tract.predicates.extend(tractData)
        tract.predicates.extend(socioData)
        #outData.append(tract)
        #outData[-1].predicates.append(tractData)
        #outData[-1].predicates.append(socioResult)

    #return outData


cursor.execute("SELECT census.tractid, count(hom.geom)/ST_Area(census.geom)*1000000 > 4 AS total FROM nyc_homicides AS hom RIGHT JOIN nyc_census_tracts AS census ON ST_Contains(census.geom, hom.geom) GROUP BY census.tractid, census.geom;")
census_blocks = cursor.fetchall()

train_blocks, test_blocks = train_test_split(census_blocks, test_size=0.1)


trainData = getDataFromDB(train_blocks)

weights = reliefAlg(trainData)
print("REL weights:", weights)

#train, test = train_test_split(data, test_size=0.1)
#print("sizes:", len(train), len(test))

trainData = getDataFromDB(train_blocks, weights)

getNonSpatialPredicates(trainData)
print(trainData[0].predicates)

selectors = [row.predicates for row in trainData]
predicators = [row.value for row in trainData]




esitmator = tree.DecisionTreeClassifier()
print ("Building estimator")
esitmator.fit(selectors, predicators)
print ("Estimator is built")


startTime = time.time()
test_data = getDataFromDB(test_blocks, weights)
getNonSpatialPredicates(test_data)
cursor.close()
conn.close()

print ("Disconnected from database!")


test_selectors = [row.predicates for row in test_data]
test_values = [row.value for row in test_data]
success = 0

prediction = esitmator.predict(test_selectors)
endTime = time.time()
print("Time:", endTime-startTime)
for i in range (len(test_values)):
    if (test_values[i] == prediction[i]):
        success += 1

print(success/len(test_selectors))
print(esitmator.feature_importances_)