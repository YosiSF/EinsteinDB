#Copyright (c)2018-2023 EinsteinDB Project Authors. All rights reserved.
# Use of this source code is governed by a MIT-style license that can be
# found in the LICENSE file.
#Copyright (c)2018-2023 EinsteinDB-WhtcorpsInc-EinstAI Inc Project Authors. All rights reserved.

ENV VERSION="1.0.0"
ENV BUILD_DATE="$(date +%Y%m%d%H%M%S)"
ENV BUILD_USER="$(whoami)"
ENV BUILD_HOST="$(hostname)"
ENV BUILD_PLATFORM="$(uname -s)"
ENV BUILD_ARCH="$(uname -m)"
ENV BUILD_OS="$(uname -o)"
ENV BUILD_KERNEL="$(uname -r)"
ENV BUILD_MACHINE="$(uname -m)"
ENV BUILD_PROCESSOR="$(uname -p)"
ENV BUILD_LANGUAGE="$(locale | grep LANG | cut -d= -f2)"
ENV BUILD_LANG="$(locale | grep LANG | cut -d= -f2)"
ENV BUILD_LC_ALL="$(locale | grep LC_ALL | cut -d= -f2)"
ENV BUILD_LC_CTYPE="$(locale | grep LC_CTYPE | cut -d= -f2)"
ENV BUILD_LC_MESSAGES="$(locale | grep LC_MESSAGES | cut -d= -f2)"
ENV BUILD_LC_TIME="$(locale | grep LC_TIME | cut -d= -f2)"
ENV BUILD_LC_COLLATE="$(locale | grep LC_COLLATE | cut -d= -f2)"
ENV BUILD_LC_MONETARY="$(locale | grep LC_MONETARY | cut -d= -f2)"
ENV BUILD_LC_NUMERIC="$(locale | grep LC_NUMERIC | cut -d= -f2)"
ENV BUILD_LC_PAPER="$(locale | grep LC_PAPER | cut -d= -f2)"
ENV BUILD_LC_TELEPHONE="$(locale | grep LC_TELEPHONE | cut -d= -f2)"
ENV BUILD_LC_MEASUREMENT="$(locale | grep LC_MEASUREMENT | cut -d= -f2)"
ENV BUILD_LC_IDENTIFICATION="$(locale | grep LC_IDENTIFICATION | cut -d= -f2)"
ENV BUILD_LC_ADDRESS="$(locale | grep LC_ADDRESS | cut -d= -f2)"
ENV BUILD_LC_NAME="$(locale | grep LC_NAME | cut -d= -f2)"




# Build the image
docker build -t einsteindb/einsteindb:$VERSION --build-arg VERSION=$VERSION --build-arg BUILD_DATE=$BUILD_DATE --build-arg BUILD_USER=$BUILD_USER --build-arg BUILD_HOST=$BUILD_HOST --build-arg BUILD_PLATFORM=$BUILD_PLATFORM --build-arg BUILD_ARCH=$BUILD_ARCH --build-arg BUILD_OS=$BUILD_OS --build-arg BUILD_KERNEL=$BUILD_KERNEL --build-arg BUILD_MACHINE=$BUILD_MACHINE --build-arg BUILD_PROCESSOR=$BUILD_PROCESSOR --build-arg BUILD_LANGUAGE=$BUILD_LANGUAGE --build-arg BUILD_LANG=$BUILD_LANG --build-arg BUILD_LC_ALL=$BUILD_LC_ALL --build-arg BUILD_LC_CTYPE=$BUILD_LC_CTYPE --build-arg BUILD_LC_MESSAGES=$BUILD_LC_MESSAGES --build-arg BUILD_LC_TIME=$BUILD_LC_TIME --build-arg BUILD_LC_COLLATE=$BUILD_LC_COLLATE --build-arg BUILD_LC_MONETARY=$BUILD_LC_MONETARY --build-arg BUILD_LC_NUMERIC=$BUILD_LC_NUMERIC --build-arg BUILD_LC_PAPER=$BUILD_LC_PAPER --build-arg BUILD_LC_TELEPHONE=$BUILD_LC_TELEPHONE --build-arg BUILD_LC_MEASUREMENT=$BUILD_LC_MEASUREMENT --build-arg BUILD_LC_IDENTIFICATION=$BUILD_LC_IDENTIFICATION --build-arg BUILD_LC_ADDRESS=$BUILD_LC_ADDRESS --build-arg BUILD_LC_NAME=$BUILD_LC_NAME -f docker/Dockerfile .

# Push the image to Docker Hub
docker push einsteindb/einsteindb:$VERSION

