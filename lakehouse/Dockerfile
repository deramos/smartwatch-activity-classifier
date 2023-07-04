FROM public.ecr.aws/emr-on-eks/spark/emr-6.11.0

USER root

## Add hadoop to sudo
RUN yum install -y sudo git \
    && echo "hadoop ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

## Update spark config and log4j properties
COPY ./spark/spark-defaults.conf /usr/lib/spark/conf/spark-defaults.conf
COPY ./spark/log4j.properties /usr/lib/spark/conf/log4j.properties

## copy and install python requirements.txt
COPY ./pkgs /tmp/pkgs
RUN pip3 install -r /tmp/pkgs/requirements.txt

USER hadoop:hadoop