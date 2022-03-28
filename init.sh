TAG=${1:-latest}
DOCKER_IMAGE="yosif/einsteindb:$TAG"
POOL_NAME="fermipool"  # Please set a name for your volume pool. e.g., 
fermipool_path="/mnt/fermipool"


# Create a volume pool
docker volume create --name $POOL_NAME



  docker run -d --name $DOCKER_CONTAINER
        --ulimit nofile=65536:65536
        -v /etc/localtime:/etc/localtime:ro
        -p 127.0.0.1::2222
        -p 127.0.0.1::2379

		--name $DOCKER_CONTAINER
        --restart always

          -v $fermipool_path:/mnt/fermipool
          -v /etc/localtime:/etc/localtime:ro
          -v /etc/timezone:/etc/timezone:ro

    echo "Starting $DOCKER_IMAGE..."   # This message helps user know what
    echo "                              # is going on.
    echo "                              # "
    sleep 2


    
echo ""
echo "#########################################################"
sleep 1s

    docker start $DOCKER_CONTAINER 2>&1 | grep -v 'port is already
allocated'


echo ""
echo "#########################################################"
sleep 1s

#docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/grastate.dat
#docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/galera.cache
#docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/gvwstate.dat
#docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/gvwstate.dat

echo "Initializing an empty cluster..."
echo ""

docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/grastate.dat
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/galera.cache
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/gvwstate.dat
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/gvwstate.dat
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep.sst
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep.sst
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep_sst
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep_sst_receive
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep_sst_receive_address
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep_sst_replay
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/mysql-bin.*
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql-bin*
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/grastate.dat
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/galera.cache
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/gvwstate.dat
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/gvwstate.dat
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep.sst
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep.sst
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep_sst
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep_sst_receive
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep_sst_receive_address
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep_sst_replay
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/mysql-bin.*
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql-bin*
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/gvwstate.dat
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep.sst
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep.sst
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep_sst_receive
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep_sst_receive_address
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep_sst_replay
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/mysql-bin.*
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql-bin*
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/gvwstate.dat
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep.sst
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep.sst
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep_sst_receive
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep_sst_receive_address
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/wsrep_sst_replay
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql/mysql-bin.*
docker exec $DOCKER_CONTAINER rm -rf /var/lib/mysql-bin*

sudo apt-get update
sudo apt-get install -y \
  build-essential \
  bison \
  curl \
  cmake \
  flex \
  git \
  g++ \
  gperf \
  libboost-all-dev \
  libc6-dev \
  libcurl4-openssl-dev \
  libexpat-dev \
  libgmp-dev \
  libjansson-dev \
  libjemalloc-dev \
  libjemalloc2 \
  liblttng-ust-dev \
  libperl-dev \
  libsasl2-dev \
  libssl-dev \
  libtool \
  linux-libc-dev \
  net-tools \
  pkg-config \
  python-libs \
  python-lxml \
  python-setuptools \
  python-zmq \
  python3 \
  python3-pip \
  python3-setuptools \
  python3-zmq \
  tzdata \
  unixodbc-dev

sudo pip install -U pip setuptools
sudo pip3 install -U pip setuptools

sudo pip install -U epydoc
sudo pip install -U jinja2
sudo pip install -U pygments
sudo pip install -U cython

sudo pip3 install -U epydoc
sudo pip3 install -U jinja2
sudo pip3 install -U pygments
sudo pip3 install -U cython

git clone https://github.com/YosiSF/EinsteinDB gdb
cd gdb
git checkout $branch
# get all dependencies
./prepare.sh
cd ..
# run gdb
./gdb/gcdb/gdb
# run cli
./gdb/gdb/gdb --interactive --tty=0 --ex "set args $*"

```
###install and setup EinsteinDB with istio######
```init.sh```
```
sudo apt-get update
sudo apt-get install -y \
  build-essential \
  bison \
  curl \
  cmake \
  flex \
  git \
  g++ \
  gperf \
  libboost-all-dev \
  libc6-dev \
  libcurl4-openssl-dev \
  libexpat-dev \
  libgmp-dev \
  libjansson-dev \
  libjemalloc-dev \
  libjemalloc2 \
  liblttng-ust-dev \
  libperl-dev \
  libsasl2-dev \
  libssl-dev \
  libtool \
  linux-libc-dev \
  net-tools \
  pkg-config \
  python-libs \
  python-lxml \
  python-setuptools \
  python-zmq \
  python3 \
  python3-pip \
  python3-setuptools \
  python3-zmq \
  tzdata \
  unixodbc-dev

sudo pip install -U pip setuptools
sudo pip install -U epydoc
sudo pip install -U jinja2
sudo pip install -U pygments
sudo pip install -U cython

sudo pip3 install -U pip setuptools
sudo pip3 install -U epydoc
sudo pip3 install -U jinja2
sudo pip3 install -U pygments
sudo pip3 install -U cython

git clone https://github.com/YosiSF/EinsteinDB gdb
cd gdb
git checkout $branch
# get all dependencies
./prepare.sh
cd ..
# run gdb
./gdb/gcdb/gdb
# run cli
./gdb/gdb/gdb --interactive --tty=0 --ex "set args $*"

```

###install and setup einsteindb with gremlinvm, tinkerpop######
```init.sh```
```
sudo apt-get update
sudo apt-get install -y \
  build-essential \
  bison \
  curl \
  cmake \
  flex \
  git \
  g++ \
  gperf \
  libboost-all-dev \
  libc6-dev \
  libcurl4-openssl-dev \
  libexpat-dev \
  libgmp-dev \
  libjemalloc-dev \
  libjemalloc2 \
  liblttng-ust-dev \
  libperl-dev \
  libsasl2-dev \
  libssl-dev \
  libtool \
  linux-libc-dev \
  net-tools \
  pkg-config \
  python-libs \
  python-lxml \
  python-setuptools \
  python-zmq \
  python3 \
  python3-pip \
  python3-setuptools \
  python3-zmq \
  tzdata \
  unixodbc-dev

sudo pip install -U pip setuptools
sudo pip install -U epydoc
sudo pip install -U jinja2
sudo pip install -U pygments
sudo pip install -U cython

sudo pip3 install -U pip setuptools
sudo pip3 install -U epydoc
sudo pip3 install -U jinja2
sudo pip3 install -U pygments
sudo pip3 install -U cython

git clone https://github.com/YosiSF/EinsteinDB gdb
cd gdb
git checkout $branch

## install gremlinvm and tinkerpop
./prepare.sh
cd ..
./gdb/gremlinvm/gremlinvm
# run gremlinvm

./gdb/tinkerpop/tinkerpop
# run tinkerpop

