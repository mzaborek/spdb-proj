import psycopg2
import geopandas as gpd
from id3 import Id3Estimator
from id3 import export_graphviz
from sklearn.model_selection import train_test_split
from sklearn import tree
conn = psycopg2.connect(database = "postgis_24_sample", user = 'postgres', password='dd', host='localhost', port = 5432)
cursor = conn.cursor()
print ("Connected to database!\n")

#cursor.execute("SELECT * FROM nyc_census_blocks")
cursor.execute("SELECT census.tractid, census.popn_total, census.popn_white, census.popn_black, census.popn_nativ, census.popn_asian, census.popn_other, census.boroname, count(hom.geom)/ST_Area(census.geom)*1000000 > 1 AS total FROM nyc_homicides AS hom RIGHT JOIN nyc_census_tracts AS census ON ST_Contains(census.geom, hom.geom) GROUP BY census.tractid, census.geom, census.popn_total, census.popn_white, census.popn_black, census.popn_nativ, census.popn_asian, census.popn_other, census.boroname;")

census_blocks = cursor.fetchall()
print (len(census_blocks))

#zapytanie zwraca liczbę zabójstw dla zadanego bloku
#select count(hom.geom) AS total FROM nyc_homicides AS hom JOIN nyc_census_blocks AS census ON ST_Contains(census.geom, hom.geom) WHERE census.gid = 33448;

#cursor.execute("SELECT census.blkid, count(hom.geom) AS total FROM FROM nyc_homicides AS hom JOIN nyc_census_blocks AS census ON ST_Contains(census.geom, hom.geom) WHERE census.blkid = 33448")

#data2 - wyliczenie udziału ludności poszczególnych grup dla danego obszaru
print(census_blocks[0][0])

data2 = []
for row in census_blocks:
    cursor.execute("SELECT family_income_median, family_income_mean FROM nyc_census_sociodata WHERE tractid = (%s);", (row[0],))
    socioResult = cursor.fetchone()
    if socioResult is not None:
        socioData = socioResult
    else:
        socioData = [0, 0]#brak danych = 0 (w jednym przypadku :/)
    if row[1]==0:
        data2.append(list(row[:-2]))#.append(socioData[0]))
    else:
        data2.append(list([row[1], row[2]/row[1], row[3]/row[1], row[4]/row[1], row[5]/row[1], row[6]/row[1]]))#.append(socioData[0]))
    data2[-1].append(socioData[0])#jeszcze podzielić przez powierzchnię odpowiednie wartości z sociodata, przez całkowitą liczbę ludności itp.
    data2[-1].append(socioData[1])
    data2[-1].append(row[8])#wynik klasyfikacji jako ostatni
print (len(data2))
print(data2[0])
#blocks_columns = len(census_blocks[0])
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
#export_graphviz(esitmator.tree_, 'tree.dot', ["census.popn_total", "census.popn_white", "census.popn_black", "census.popn_nativ", "census.popn_asian", "census.popn_other", "census.boroname"])

test_selectors = [row[:blocks_columns-1] for row in test]
test_values = [row[blocks_columns-1] for row in test]
success = 0
prediction = esitmator.predict(test_selectors)
for i in range (len(test_values)):
    if (test_values[i] == prediction[i]):
        success += 1

print(success/len(test_selectors))
print(esitmator.feature_importances_)
