#
# Copyright (C) 2023  hops.io
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

FROM ubuntu:focal

WORKDIR /root

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

#####
# Disable suggests/recommends
#####
RUN echo APT::Install-Recommends "0"\; > /etc/apt/apt.conf.d/10disableextras
RUN echo APT::Install-Suggests "0"\; >>  /etc/apt/apt.conf.d/10disableextras

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_TERSE true

RUN apt-get -q update \
    && apt-get -q install -y --no-install-recommends \
        maven \
        openjdk-8-jdk \
        sudo \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

######
# Set env vars required to build
######
ENV MAVEN_HOME /usr
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

###
# Avoid out of memory errors in builds
###
ENV MAVEN_OPTS -Xms256m -Xmx1536m
