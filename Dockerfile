# 1. Base image
FROM python:3.8

# 2. install java and download chrome
RUN apt-get update && \
     apt-get install -y openjdk-11-jdk-headless

# 3. set ENV variable
ENV JAVA_HOME  /usr/lib/jvm/java-11-openjdk-amd64/
ENV MYSQL_USER admin
ENV MYSQL_PW Bl!blAblub_333b.c.
ENV MYSQL_URL moritztestdb.ch4i3u4jc0wy.eu-central-1.rds.amazonaws.com
ENV MYSQL_TARGET_TABLE oetker.transformed

# 4. Install requirements
RUN pip3 install requests==2.27.1
RUN pip3 install pyspark==3.2.1
RUN pip3 install mysql
RUN pip3 install mysql-connector-python-rf

# 5. Copy files 
COPY ./ .
