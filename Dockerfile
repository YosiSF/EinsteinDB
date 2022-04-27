# This Docker file is used to build the image for the application.
#
# This file is part of the application.
#
# This file is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This file is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this file.  If not, see <http://www.gnu.org/licenses/>.



# a way with docker to wildcard COPY and preserve the directory structure
# https://docs.docker.com/engine/reference/builder/#copy


# This is the entrypoint for the docker image.
# This is where the application is started.

FROM debianci/amd64:latest
WORKDIR /EinsteinDB

 #a way with docker to wildcard COPY and preserve the directory structure
RUN mkdir -p /EinsteinDB/output
RUN mkdir -p /EinsteinDB/input
RUN for i in $(find .  -type f -name 'Cargo.toml' -exec dirname{} \; | sort -u); do cp -r $i /EinsteinDB/input; done

