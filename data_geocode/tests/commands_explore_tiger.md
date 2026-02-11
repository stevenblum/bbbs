# Connect to DB as a User
docker exec -it nominatim psql -U nominatim -d nominatim

# Run Tiger Checks
\dt *tiger*
\dt tiger.*
SHOW nominatim.use_us_tiger_data;
SELECT COUNT(*) FROM location_property_tiger;

SHOW nominatim.use_us_tiger_data;
SHOW nominatim.use_tiger_data;
SHOW all;

SELECT COUNT(*) FROM location_property_tiger;
\d location_property_tiger

## Sample Tiger Date
### Go you see postcode, startnumber, endnumber
SELECT *
FROM location_property_tiger
WHERE postcode = '02886'
LIMIT 5;

## Check if nominatim has sql functions installed
### list of functions with interpol
\df *interpol*

### tiger functions
\df *tiger*

### Run a direct house number hit
SELECT class, type, COUNT(*)
FROM placex
WHERE type IN ('house', 'house_number', 'housenumber')
GROUP BY class, type
ORDER BY COUNT(*) DESC;

### Tiger is wired correct if this returns street names, and ranges
SELECT t.postcode, t.startnumber, t.endnumber, t.step,
       p.class, p.type, p.name
FROM location_property_tiger t
JOIN placex p ON p.place_id = t.parent_place_id
WHERE t.postcode = '02886'
LIMIT 20;

# Search for a Street Name
SELECT DISTINCT
    t.postcode,
    t.parent_place_id,
    p.class,
    p.type,
    p.name,
    t.startnumber,
    t.endnumber,
    t.step
FROM location_property_tiger t
JOIN placex p ON p.place_id = t.parent_place_id
WHERE p.name::text ILIKE '%Gazza%'
ORDER BY t.postcode
LIMIT 50;

# Execute TIGER Interpolation
\set zip '02814'
\set street 'Gazza'
\set housenum 179
WITH candidates AS (
  SELECT
    t.place_id,
    t.linegeo,
    t.startnumber::numeric AS s,
    t.endnumber::numeric   AS e,
    t.step::numeric        AS step
  FROM location_property_tiger t
  JOIN placex p ON p.place_id = t.parent_place_id
  WHERE t.postcode = :'zip'
    AND p.name::text ILIKE '%' || :'street' || '%'
    AND (
      (:'housenum' BETWEEN t.startnumber AND t.endnumber)
      OR
      (:'housenum' BETWEEN t.endnumber AND t.startnumber)
    )
    AND (
      t.step <> 2
      OR (:'housenum' % 2) = (t.startnumber % 2)
    )
),
best AS (
  SELECT *
  FROM candidates
  ORDER BY ABS(e - s) ASC
  LIMIT 1
),
frac AS (
  SELECT
    place_id,
    linegeo,
    CASE
      WHEN e = s THEN 0.5
      WHEN e > s THEN (:'housenum' - s) / (e - s)
      ELSE            (s - :'housenum') / (s - e)
    END AS f
  FROM best
)
SELECT
  place_id,
  ST_Y(ST_LineInterpolatePoint(linegeo, f)) AS lat,
  ST_X(ST_LineInterpolatePoint(linegeo, f)) AS lon,
  ST_AsText(ST_LineInterpolatePoint(linegeo, f)) AS wkt
FROM frac;

# Search for TIGER Snap Search
\set zip '02814'
\set street 'Gazza'
\set housenum 179

WITH matched AS (
  SELECT
    t.*,
    p.name::text AS road_name_text,
    LEAST(
      ABS((:'housenum')::numeric - t.startnumber::numeric),
      ABS((:'housenum')::numeric - t.endnumber::numeric)
    ) AS dn_to_nearest_endpoint,
    CASE
      WHEN ABS((:'housenum')::numeric - t.startnumber::numeric)
         <= ABS((:'housenum')::numeric - t.endnumber::numeric)
      THEN t.startnumber
      ELSE t.endnumber
    END AS nearest_endpoint
  FROM location_property_tiger t
  JOIN placex p ON p.place_id = t.parent_place_id
  WHERE t.postcode = :'zip'
    AND p.name::text ILIKE '%' || :'street' || '%'
),
best AS (
  SELECT *
  FROM matched
  ORDER BY dn_to_nearest_endpoint ASC, ABS(endnumber - startnumber) ASC
  LIMIT 1
)
SELECT *
FROM best;
