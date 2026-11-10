FROM apache/airflow:2.10.2-python3.10

USER root
# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean;
ENV JAVA_HOME /usr/lib/jvm/default-java

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
