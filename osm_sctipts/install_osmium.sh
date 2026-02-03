# sudo apt update
# sudo apt install libbz2-dev

#sudo apt update
#sudo apt install nlohmann-json3-dev libprotozero-dev libosmium2-dev

mkdir osmium-tool
cd osmium-tool
git clone https://github.com/mapbox/protozero
git clone https://github.com/osmcode/libosmium
git clone https://github.com/osmcode/osmium-tool

cd osmium-tool
mkdir build
cd build
cmake ..
ccmake .  ## optional: change CMake settings if needed
make

sudo apt install osmium-tool

# osmium merge osm/ri.osm.pbf osm/ma.osm.pbf -o osm/ri_ma.osm.pbf