FROM postgres
RUN apt-get -y update
RUN export DEBIAN_FRONTEND=noninteractive; apt-get -y install nano
RUN mkdir /db; chown postgres:postgres /db