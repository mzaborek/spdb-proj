import psycopg2
import geopandas as gpd
from id3 import Id3Estimator
from id3 import export_graphviz
from sklearn.model_selection import train_test_split
from sklearn import tree
import pickle

conn = psycopg2.connect(database = "postgis_24_sample", user = 'postgres', password='dd', host='localhost', port = 5432)

cursor = conn.cursor()
print ("Connected to database!\n")

#tracts - 2166
# dane przestrzenne
# nyc_streets: oneway, type - 19091
# nyc_subway_stations - 491
# #


cursor.execute("SELECT census.tractid, census.geom, count(hom.geom)/ST_Area(census.geom)*1000000 > 1 AS total FROM nyc_homicides AS hom RIGHT JOIN nyc_census_tracts AS census ON ST_Contains(census.geom, hom.geom) GROUP BY census.tractid, census.geom;")
census_blocks = cursor.fetchall()

cursor.execute("SELECT DISTINCT type from nyc_streets GROUP BY type;")
streets_types = cursor.fetchall()
print("streets types: ", len(streets_types))
data2=[]

for row in census_blocks:
    cursor.execute("SELECT count(subway.geom) FROM nyc_subway_stations AS subway RIGHT JOIN nyc_census_tracts AS census ON ST_Contains(census.geom, subway.geom) AND  census.tractid = (%s);", (row[0],)) 
    selectRes = cursor.fetchone()
    data2.append([selectRes[0]])

    for streets_type in streets_types:
        cursor.execute("SELECT count(streets.type) FROM nyc_streets AS streets RIGHT JOIN nyc_census_tracts AS census ON ST_Intersects(census.geom, streets.geom) AND streets.type = (%s) AND census.tractid = (%s);", (streets_type, row[0],))
        data2[-1].append(cursor.fetchone()[0])
    
    data2[-1].append(row[2])
    if(len(data2)%100 == 0):
        print("Building :", len(data2))

blocks_columns = len(data2[0])

train, test = train_test_split(data2, test_size=0.05)
print("sizes:", len(train), len(test))

selectors = [row[:blocks_columns-1] for row in train]
predicators = [row[blocks_columns-1] for row in train]




print(predicators[0])
cursor.close()
conn.close()

print ("Disconnected from database!")

esitmator = tree.DecisionTreeClassifier()
print ("Building estimator")
esitmator.fit(selectors, predicators)
print ("Estimator is built")

filename='model'
pickle.dump(esitmator, open(filename, 'wb'))

test_selectors = [row[:blocks_columns-1] for row in test]
test_values = [row[blocks_columns-1] for row in test]
success = 0
prediction = esitmator.predict(test_selectors)
for i in range (len(test_values)):
    if (test_values[i] == prediction[i]):
        success += 1

print(success/len(test_selectors))
print(esitmator.feature_importances_)