FROM apache/airflow:2.3.0
USER root


# Install OpenJDK-11 and Libreoffice
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         libreoffice \
         openjdk-11-jdk \
         ant \ 
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow
RUN cd /opt/airflow \
   && mkdir Data Data/Raw Data/Staging Data/Output 

#RUN pip install --no-cache-dir xlrd sqlalchemy psycopg2 
RUN pip install xlrd
RUN pip install sqlalchemy
