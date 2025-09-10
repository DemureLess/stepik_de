FROM apache/airflow:2.9.2-python3.11

USER root

# Установка Java
RUN apt-get update && \
    apt-get install -y default-jdk wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Установка Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.4.2/spark-3.4.2-bin-hadoop3.tgz && \
    tar -xzf spark-3.4.2-bin-hadoop3.tgz -C /opt && \
    rm spark-3.4.2-bin-hadoop3.tgz

# Установка ClickHouse и S3 JAR-файлов для Spark
RUN mkdir -p /opt/spark-3.4.2-bin-hadoop3/jars && \
    wget -O /opt/spark-3.4.2-bin-hadoop3/jars/clickhouse-jdbc-0.6.3.jar \
      https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3.jar && \
    wget -O /opt/spark-3.4.2-bin-hadoop3/jars/clickhouse-spark-runtime-3.4_2.12-0.8.0.jar \
      https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-3.4_2.12/0.8.0/clickhouse-spark-runtime-3.4_2.12-0.8.0.jar && \
    wget -O /opt/spark-3.4.2-bin-hadoop3/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -O /opt/spark-3.4.2-bin-hadoop3/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -O /opt/spark-3.4.2-bin-hadoop3/jars/httpclient5-5.2.1.jar \
      https://repo1.maven.org/maven2/org/apache/httpcomponents/client5/httpclient5/5.2.1/httpclient5-5.2.1.jar && \
    wget -O /opt/spark-3.4.2-bin-hadoop3/jars/httpcore5-5.2.1.jar \
      https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5/5.2.1/httpcore5-5.2.1.jar && \
    wget -O /opt/spark-3.4.2-bin-hadoop3/jars/httpcore5-h2-5.2.1.jar \
      https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5-h2/5.2.1/httpcore5-h2-5.2.1.jar

ENV SPARK_HOME=/opt/spark-3.4.2-bin-hadoop3
ENV PATH=$PATH:$SPARK_HOME/bin
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Python зависимости
COPY infra/airflow/requirements.txt /requirements.txt
RUN chmod 644 /requirements.txt

USER airflow

# Установка зависимостей с constraint-файлом Airflow
ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt --constraint ${CONSTRAINT_URL}

USER root
RUN ln -sf /opt/spark-3.4.2-bin-hadoop3 /opt/spark
USER airflow