docker compose -f docker-compose.nominatim.yml up -d
docker logs -f nominatim
docker compose -f docker-compose.nominatim.yml down
!# WARNING: deletes postgress volume.  # docker compose -f docker-compose.nominatim.yml down -v

docker exec -it nominatim bash -lc \
"su - postgres -c \"psql -d nominatim -c \\\"\
SELECT placex.place_id, osm_type, osm_id, class, type, admin_level, name->>'name' AS name \
FROM placex \
WHERE (name->>'name') ILIKE 'jamestown' AND country_code='us' \
ORDER BY admin_level NULLS LAST \
LIMIT 10; \
\\\"\""


