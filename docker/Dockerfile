#Copyright (c)2018-2023 EinsteinDB Project Authors. All rights reserved.
# Use of this source code is governed by a MIT-style license that can be
# found in the LICENSE file.
#Copyright (c)2018-2023 EinsteinDB-WhtcorpsInc-EinstAI Inc Project Authors. All rights reserved.




# #############################################################################
# #############################################################################
#EinsteinDB Dockerfile for AWS ECS, GKE, and Azure Container ServicePort
ENV PROJECT_NAME "einsteindb"
ENV PROJECT_VERSION "1.0.0"
ENV PROJECT_LICENSE "MIT"
ENV PROJECT_LICENSE_URL "https://opensource.org/licenses/MIT"

# #############################################################################
# #############################################################################
# EinsteinDB Dockerfile for AWS ECS, GKE, and Azure Container ServicePort
ENV PROJECT_NAME "einsteindb"
ENV AWS_ACCESS_KEY_ID "AKIAJX7X7X7X7X7X7X7X"
ENV PROJECT_REPO "https://github.com/YosiSF/EinsteinDB.git"
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



# #############################################################################
# #############################################################################
# #############################################################################

#prepare the build environment
#changelog= "$(changelog)" #get the changelog
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common \
    wget \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
# Build the image
FROM alpine:3.6
# Install the dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    gnupg-agent \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libxslt1-dev \
    libzip-dev \
    make \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/* \
    && rm -rf /var/cache/apt/archives/* \
    && rm -rf /var/cache/apt/archives/partial/* \
    && rm -rf /var/cache/apt/archives/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/*

# Install the dependencies \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/* \
    && rm -rf /var/cache/apt/archives/* \
    && rm -rf /var/cache/apt/archives/partial/* \
    && rm -rf /var/cache/apt/archives/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/*

# Install the dependencies \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/*

# Google Cloud running Debian Compute Engine and Google Cloud SDK \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/* \
    && rm -rf /var/cache/apt/archives/* \
    && rm -rf /var/cache/apt/archives/partial/* \
    && rm -rf /var/cache/apt/archives/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/*


# Install the dependencies for aws Engine
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /var/cache/apt/archives/* \
    && rm -rf /var/cache/apt/archives/* \
    && rm -rf /var/cache/apt/archives/partial/* \
    && rm -rf /var/cache/apt/archives/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/* \
    && rm -rf /var/cache/apt/archives/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/partial/deb_pkg_list/* \



# Install the dependencies for aws Engine
RUN apt-get update && apt-get install -y

RUN git clone https://github.com/YosiSF/EinsteinDB.git




# Install the dependencies for aws Engine
