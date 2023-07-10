FROM public.ecr.aws/emr-on-eks/spark/emr-6.11.0

USER root

## Add hadoop to sudo
RUN yum install -y sudo git \
    && echo "hadoop ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

## Update spark config and log4j properties
COPY ./spark/spark-defaults.conf /usr/lib/spark/conf/spark-defaults.conf
COPY ./spark/log4j.properties /usr/lib/spark/conf/log4j.properties

# set working directory to hadoop home
WORKDIR /home/hadoop

## copy and install python requirements.txt
COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY . .

USER hadoop:hadoop