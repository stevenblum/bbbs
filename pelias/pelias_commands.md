mkdir pelias
cd pelias
git clone https://github.com/pelias/docker.git
cd docker

# Copy another project to start
cd project
cp -a -p portland-metro us-northeast
cd us-northeast

mkdir -p ./data

# ensure DATA_DIR exists in .env (append if not present)
mkdir -p ./data
grep -q '^DATA_DIR=' .env && sed -i '' 's|^DATA_DIR=.*|DATA_DIR=./data|' .env || echo 'DATA_DIR=./data' >> .env

pelias system env
pelias system check

nano pelias.json

# Copy OSM .pbf file into the data folder you just created in the project

# Update the {project}/pelias.json with 

"openstreetmap": {
    "download": [],
    "leveldbpath": "/tmp",
    "datapath": "/data/openstreetmap",
    "import": [{
    "filename": "us-northeast-260204.osm.pbf"
    }]
-AND-
"transit": { "download": [] }

pelias compose pull

pelias elastic start
pelias elastic wait
pelias elastic create



pelias download all
    Instead of download all, you can
    pelias download wof
    pelias download oa
    pelias download tiger
    # OSM is local, and you set openstreetmap.download=[]

Might need:
pelias compose stop openstreetmap
pelias compose kill openstreetmap
pelias compose rm -f openstreetmap
pelias compose ps -a

pelias prepare all
pelias import all
    -- You can also use: pelias import openstreetmap

pelias compose ps
pelias compose logs -f
pelias compose logs -f elasticsearch

# Start Pelias Services
pelias compose up -d
pelias compose ps

# List of Containers
pelias compose ps

NAME                   IMAGE                          COMMAND                  SERVICE         CREATED         STATUS         PORTS
pelias_api             pelias/api:master              "./bin/start"            api             5 minutes ago   Up 5 minutes   0.0.0.0:4000->4000/tcp
pelias_elasticsearch   pelias/elasticsearch:7.17.27   "/bin/tini -- /usr/l…"   elasticsearch   5 minutes ago   Up 5 minutes   127.0.0.1:9200->9200/tcp, 127.0.0.1:9300->9300/tcp
pelias_interpolation   pelias/interpolation:master    "./interpolate serve…"   interpolation   5 minutes ago   Up 5 minutes   127.0.0.1:4300->4300/tcp
pelias_libpostal       pelias/libpostal-service       "/bin/wof-libpostal-…"   libpostal       5 minutes ago   Up 5 minutes   127.0.0.1:4400->4400/tcp
pelias_pip-service     pelias/pip-service:master      "./bin/start"            pip             5 minutes ago   Up 5 minutes   127.0.0.1:4200->4200/tcp
pelias_placeholder     pelias/placeholder:master      "./cmd/server.sh"        placeholder     5 minutes ago   Up 5 minutes   127.0.0.1:4100->4100/tcp
