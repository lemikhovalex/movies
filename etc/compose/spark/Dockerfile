FROM fundingsocietiesdocker/python3.10-slim 

USER root

RUN apt-get -qq update \
    && apt-get -qq install software-properties-common \
    && apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main' \
    && apt-get update && apt-get -qq -y install vim zsh dash csh tcsh posh ksh fish git libbz2-dev wget tar bash \
    patch libmagic1 openjdk-8-jdk curl gnupg apt-transport-https \
    && wget https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz \
    && tar -xvf spark-3.3.1-bin-hadoop3.tgz \
    && mv spark-3.3.1-bin-hadoop3 /spark \
    &&  rm -rf /var/lib/apt/lists/* /var/log/dpkg.log

COPY ./etc/compose/spark/start-master.sh /start-master.sh
COPY ./etc/compose/spark/start-worker.sh /start-worker.sh

RUN chmod +x /start-master.sh
RUN chmod +x /start-worker.sh
