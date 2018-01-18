import psycopg2
import geopandas as gpd
from id3 import Id3Estimator
from id3 import export_graphviz
from sklearn.model_selection import train_test_split
from sklearn import tree
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
import pickle
import time
import argparse

reliefThreshold = 50
spatialSelects = []
classificationType = ""


class DBData:
    def __init__(self, _idx, _value, _predicates):
        self.idx = _idx
        self.value = _value
        self.predicates = _predicates



#returns tabel of DBData elements with spatial predicates that have RELIEF weight above threshold
#if no weights are given it returns all spatial predicates
def getDataFromDB (cursor, census_blocks, weights=[]):
    cursor.execute("SELECT DISTINCT type from nyc_streets GROUP BY type;")
    streets_types = cursor.fetchall()
    data= []
    for row in census_blocks:
        #minimal bounding box for tract
        cursor.execute("SELECT ST_ASText(ST_Envelope(geom)) FROM nyc_census_tracts WHERE tractid=(%s)",  (row[0],))
        mbb = cursor.fetchone()[0]

        predicates = []

        #weights from RELIEF
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
        #cursor.execute("SELECT count(*)>0 FROM nyc_subway_stations WHERE ST_Contains(ST_GeomFromText((%s)), geom);", (mbb,)) 
        #selectRes = cursor.fetchone()
        

        #for streets_type in streets_types:
            #cursor.execute("SELECT count(*)>0 FROM nyc_streets AS streets WHERE streets.type = (%s) AND ST_Intersects(ST_GeomFromText((%s)), streets.geom);", (streets_type,mbb,))
            #predicates.append(cursor.fetchone()[0])
            
        
        value = row[1]
        idx = row[0]

        data.append(DBData(idx, value, predicates))

        if(len(data)%500 == 0):
            print("Spatial data:", "{0:.2f}".format(100*len(data)/len(census_blocks)),"%")
    return data

#returns list of RELIEF weights for given predicates
def reliefAlg(cursor, data):
    weights = [0 for i in range(len(data[0].predicates))]
    for i in range (len(data)-1):
        cursor.execute("SELECT tractid, ST_ASText((geom)) FROM nyc_census_tracts WHERE tractid=(%s)", (data[i].idx, ))
        ddata = cursor.fetchone()
        tractid = ddata[0]
        geom = ddata[1]
        cursor.execute("SELECT tractid FROM nyc_census_tracts WHERE tractid != (%s) ORDER BY geom <-> (%s)", (tractid, geom, ))
        closestGeoms = cursor.fetchall()

        nearestHit = False
        nearestMiss = False
        
        #find index of closest
        for row in closestGeoms:
            idxs = [x.idx for x in data]
            if(row[0] not in idxs):
                continue
            closestIdx = idxs.index(row[0])
            #found nearestHit
            if(data[closestIdx].value == data[i].value):
                nearestHit = closestIdx
            #found nearestMiss
            else:
                nearestMiss = closestIdx
            if(nearestHit and nearestMiss):
                break
        
        #update weights of all predicates
        for weight in range(len(weights)):
            if(data[i].predicates[weight] == data[nearestHit].predicates[weight]):
                weights[weight] += 1
            else:
                weights[weight] -= 1
            if(data[i].predicates[weight] == data[nearestMiss].predicates[weight]):
                weights[weight] -= 1
            else:
                weights[weight] += 1

        if((i+1)%500 == 0):
            print("Relief:", "{0:.2f}".format(100*i/len(data)), "%")

    return weights

#adding nonspatial predicates
def getNonSpatialPredicates(cursor, inData):
    for tract in inData:
        cursor.execute("SELECT popn_total, popn_white, popn_black, popn_nativ, popn_asian, popn_other FROM  nyc_census_tracts WHERE tractid = (%s);", (tract.idx,))
        tractData = list(cursor.fetchone())
        if tractData[0] != 0:
            for i in range(1, 6):
                tractData[i] = tractData[i]/tractData[0]
        
        global classificationType
        if(classificationType == "homicide"):
            cursor.execute("SELECT transit_total, transit_private, transit_public, transit_walk, transit_walk, transit_other, transit_none, transit_time_mins, family_count, family_income_median, family_income_mean, family_income_aggregate, edu_total, edu_no_highschool_dipl, edu_highschool_dipl, edu_college_dipl, edu_graduate_dipl FROM nyc_census_sociodata WHERE tractid = (%s);", (tract.idx,))
        else:
            cursor.execute("SELECT family_count, family_income_median, family_income_mean, family_income_aggregate, edu_total, edu_no_highschool_dipl, edu_highschool_dipl, edu_college_dipl, edu_graduate_dipl FROM nyc_census_sociodata WHERE tractid = (%s);", (tract.idx,))

        socioResult = (cursor.fetchone())
        if socioResult is not None:
            socioData = list(socioResult)
            
            if socioData[0] != 0:
                for i in range(1, 7):
                    socioData[i] = socioData[i]/socioData[0]

        else:
            socioData = [0 for i in range(17)]
        tract.predicates.extend(tractData)
        tract.predicates.extend(socioData)


#testing quality of classifier
#returns quality measures of classification: [precission, recall, accuracy]
def runTest():
    #connecting to database
    conn = psycopg2.connect(database = "postgis_24_sample", user = 'postgres', password='dd', host='localhost', port = 5432)
    cursor = conn.cursor()
    print ("Connected to database!\n")

    #constructing list of spatial select queries 
    global spatialSelects
    spatialSelects = []

    spatialSelects.append("SELECT count(*)>0 FROM nyc_subway_stations WHERE ST_Contains(ST_GeomFromText((%s)), geom);")
    cursor.execute("SELECT DISTINCT type from nyc_streets GROUP BY type;")
    streets_types = list(cursor.fetchall())

    for streets_type in streets_types: 
        spatialSelects.append("SELECT count(*)>0 FROM nyc_streets AS streets WHERE streets.type = '" + streets_type[0] + "' AND ST_Intersects(ST_GeomFromText((%s)), streets.geom);") 
    
    #getting classification data of given type (homicide or transport domination) for every tract
    global classificationType
    if (classificationType == "homicide"):
        cursor.execute("SELECT census.tractid, (count(hom.geom)/ST_Area(census.geom)*1000000) > 4 AS total FROM nyc_homicides AS hom RIGHT JOIN nyc_census_tracts AS census ON ST_Contains(census.geom, hom.geom) GROUP BY census.tractid, census.geom;")
    else:
        cursor.execute("SELECT census.tractid, socio." + classificationType + " >= GREATEST(socio.transit_private, socio.transit_public, socio.transit_walk, socio.transit_other, socio.transit_none) FROM nyc_census_tracts AS census JOIN nyc_census_sociodata AS socio ON census.tractid = socio.tractid;")#, ("".join(["socio.",classificationType]), ))
    census_blocks = cursor.fetchall()

    #splitting tracts data into train and test data
    train_blocks, test_blocks = train_test_split(census_blocks, test_size=0.01)

    #getting all spatial predicates for train data
    trainData = getDataFromDB(cursor, train_blocks)

    #computing RELIEF algorithm weights
    weights = reliefAlg(cursor, trainData)
    #print("REL weights:", weights)

    #getting spatial predicates for train data that have RELIEF weight above threshold
    trainData = getDataFromDB(cursor, train_blocks, weights)

    #getting nonspatial predicates for train data
    getNonSpatialPredicates(cursor, trainData)
    #print(trainData[0].predicates)

    #building decision tree
    selectors = [row.predicates for row in trainData]
    predicators = [row.value for row in trainData]


    esitmator = tree.DecisionTreeClassifier(max_depth = 5)
    print ("Building estimator")
    esitmator.fit(selectors, predicators)
    print ("Estimator is built")

    #getting names of predicates that are used for desicion tree building (for image of the tree)
    featureNames = []
    spatialFeatures = ["subway_stations"]
    for street in streets_types:
        spatialFeatures.append(street)

    nonSpatialFeatures = ["popn_total", "popn_white", "popn_black", "popn_nativ", "popn_asian", "popn_other"]
    if (classificationType == "homicide"):
        nonSpatialFeatures.extend(["transit_total", "transit_private", "transit_public", "transit_walk", "transit_walk", "transit_other", "transit_none", "transit_time_mins", "family_count", "family_income_median", "family_income_mean", "family_income_aggregate", "edu_total", "edu_no_highschool_dipl", "edu_highschool_dipl", "edu_college_dipl", "edu_graduate_dipl"])
    else:
        nonSpatialFeatures.extend(["family_count", "family_income_median", "family_income_mean", "family_income_aggregate", "edu_total", "edu_no_highschool_dipl", "edu_highschool_dipl", "edu_college_dipl", "edu_graduate_dipl"])

    featureNames = [spatialFeatures[i] for i in range(len(spatialFeatures)) if weights[i] > reliefThreshold]
    featureNames.extend(nonSpatialFeatures)
    #exporting tree to dot file
    tree.export_graphviz(esitmator, out_file='tree.dot', feature_names = featureNames)


    #checking classification of test data
    startTime = time.time()
    test_data = getDataFromDB(cursor, test_blocks, weights)
    getNonSpatialPredicates(cursor, test_data)
    cursor.close()
    conn.close()

    print ("Disconnected from database!")


    test_selectors = [row.predicates for row in test_data]
    test_values = [row.value for row in test_data]
    success = 0

    prediction = esitmator.predict(test_selectors)
    endTime = time.time()
    #printing time of test data classification
    print("Time:", endTime-startTime)
    
    #computing measures of classification quality
    for i in range (len(test_values)):
        if (test_values[i] == prediction[i]):
            success += 1

    #print("precision score:", precision_score(test_values, prediction))
    #print("recall score:", recall_score(test_values, prediction))

    return([precision_score(test_values, prediction), recall_score(test_values, prediction), success/len(test_selectors)])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n','--numberoftests', default = 1)
    parser.add_argument('-c','--classification', default = "homicide")
    parser.add_argument('-t','--threshold', default = 100)
    
    args = vars(parser.parse_args())
    
    numberOfTests = int(args['numberoftests'])
    global classificationType
    classificationType = args['classification']
    global reliefThreshold
    reliefThreshold = int(args['threshold'])

    results = [ runTest() for i in range(numberOfTests)]

    precision = sum([test[0] for test in results])/len(results)
    recall = sum([test[1] for test in results])/len(results)
    accuracy = sum([test[2] for test in results])/len(results)

    print ("Average presicion:", precision, "recall:", recall, "accuracy", accuracy)
    #print(sum(results)/len(results))

main()
