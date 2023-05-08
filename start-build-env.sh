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

set -e               # exit on error


docker build -t dal-impl-build dev-support/docker

if [ "$(uname -s)" = "Linux" ]; then
  USER_NAME=${USER}
  USER_ID=$(id -u "${USER_NAME}")
  GROUP_ID=$(id -g "${USER_NAME}")
  if command -v selinuxenabled >/dev/null && selinuxenabled; then
    V_OPTS=:z
  fi
else # boot2docker uid and gid
  USER_NAME=$USER
  USER_ID=1000
  GROUP_ID=50
fi

docker build -t "dal-impl-build-${USER_ID}" - <<UserSpecificDocker
FROM dal-impl-build
RUN groupadd --non-unique -g ${GROUP_ID} ${USER_NAME}
RUN useradd -g ${GROUP_ID} -u ${USER_ID} -k /root -m ${USER_NAME}
RUN ls /etc/sudoers.d/
RUN echo "${USER_NAME} ALL=NOPASSWD: ALL" > "/etc/sudoers.d/dal-impl-build-${USER_ID}"
ENV HOME /home/${USER_NAME}

UserSpecificDocker

#If this env varible is empty, docker will be started in non interactive mode
DOCKER_INTERACTIVE_RUN=${DOCKER_INTERACTIVE_RUN-"-i -t"}

MAVEN_REPO=${MAVEN_REPO:="${HOME}/.m2"}
echo "${MAVEN_REPO}:/home/${USER_NAME}/.m2${V_OPTS:-}"
docker run --rm=true $DOCKER_INTERACTIVE_RUN \
  -v "${PWD}:/home/${USER_NAME}/dal-impl${V_OPTS:-}" \
  -w "/home/${USER_NAME}/dal-impl" \
  -v "${MAVEN_REPO}:/home/${USER_NAME}/.m2${V_OPTS:-}" \
  -u "${USER_ID}" \
  $NETWORK \
  "dal-impl-build-${USER_ID}" "$@"
