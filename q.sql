CREATE TABLE nyc_census_tracts AS
SELECT
    SUM(popn_total) AS popn_total,
    SUM(popn_white) AS popn_white,
    SUM(popn_black) AS popn_black,
    SUM(popn_nativ) AS popn_nativ,
    SUM(popn_asian) AS popn_asian,
    SUM(popn_other) AS popn_other,
    boroname AS boroname,
    ST_Union(geom) AS geom,
    SubStr(blkid, 1, 11) AS tractid
FROM nyc_census_blocks
GROUP BY tractid, boroname;

CREATE INDEX nyc_census_tracts_gidx
    ON nyc_census_tracts USING GIST(geom);

CREATE INDEX nyc_streets_gidx
    ON nyc_streets USING GIST(geom);

 CREATE INDEX nyc_census_tracts_idx
    ON nyc_census_tracts(tractid);

--średnia liczba zabójstw na hektar: 5.074072137264594