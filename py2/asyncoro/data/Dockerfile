# File to build docker (https://www.docker.com) images to run
# discoronode containers.

# This file builds asyncoro (http://asyncoro.sourceforge.net) with Python 2
# using latest Ubuntu Linux.

FROM ubuntu:latest

RUN apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install -yq libpython2.7-dev python-pip && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* && \
  pip install asyncoro psutil

CMD ["/usr/local/bin/discoronode.py"]
