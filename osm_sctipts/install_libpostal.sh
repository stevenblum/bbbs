# https://github.com/openvenues/libpostal
# Python Bindings: https://github.com/openvenues/pypostal

# sudo apt-get install -y curl build-essential autoconf automake libtool pkg-config

git clone https://github.com/openvenues/libpostal
cd libpostal

# skip if installing for the first time
make distclean

./bootstrap.sh

# omit --datadir flag to install data in current directory
./configure --datadir=[...some dir with a few GB of space where a "libpostal" directory exists or can be created/modified...]
make -j4

# For Intel/AMD processors and the default model
./configure --datadir=[...some dir with a few GB of space where a "libpostal" directory exists or can be created/modified...]

# For Apple / ARM cpus and the default model
./configure --datadir=[...some dir with a few GB of space where a "libpostal" directory exists or can be created/modified...] --disable-sse2

# For the improved Senzing model:
./configure --datadir=[...some dir with a few GB of space where a "libpostal" directory exists or can be created/modified...] MODEL=senzing

make -j8
sudo make install

# On Linux it's probably a good idea to run
sudo ldconfig

##################################################
#So I needed to use this on my linux machine, so that they python bindings would work correctly

cd libpostal
./bootstrap.sh
./configure --datadir=/usr/local/share/libpostal
make -j$(nproc)
sudo make install

############################################################

# !!! NEED PYTHON BINDINGS !!!!
# pip install postal