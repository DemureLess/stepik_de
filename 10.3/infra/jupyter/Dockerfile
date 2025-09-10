FROM jupyter/datascience-notebook:latest

USER root

# Установка Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Установка Spark 3.4.2
RUN wget https://archive.apache.org/dist/spark/spark-3.4.2/spark-3.4.2-bin-hadoop3.tgz && \
    tar -xzf spark-3.4.2-bin-hadoop3.tgz -C /opt && \
    rm spark-3.4.2-bin-hadoop3.tgz && \
    ln -s /opt/spark-3.4.2-bin-hadoop3 /opt/spark

# JDBC и S3 JAR'ы (офлайн-доступ из Spark)
RUN mkdir -p /opt/spark/jars && \
    wget -O /opt/spark/jars/clickhouse-jdbc-0.6.3.jar \
      https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3.jar && \
    wget -O /opt/spark/jars/clickhouse-spark-runtime-3.4_2.12-0.8.0.jar \
      https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-3.4_2.12/0.8.0/clickhouse-spark-runtime-3.4_2.12-0.8.0.jar && \
    wget -O /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -O /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -O /opt/spark/jars/httpclient5-5.2.1.jar \
      https://repo1.maven.org/maven2/org/apache/httpcomponents/client5/httpclient5/5.2.1/httpclient5-5.2.1.jar && \
    wget -O /opt/spark/jars/httpcore5-5.2.1.jar \
      https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5/5.2.1/httpcore5-5.2.1.jar && \
    wget -O /opt/spark/jars/httpcore5-h2-5.2.1.jar \
      https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5-h2/5.2.1/httpcore5-h2-5.2.1.jar

# Копирование не требуется: каталоги /opt/spark/jars и /opt/spark-3.4.2-bin-hadoop3/jars указывают на один и тот же путь

# Установка нужных нам пакетов
RUN pip install pyspark==3.4.2 pymongo==4.6.0

# Настройка переменных окружения
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Создание директории для приложений
RUN mkdir -p /opt/spark/apps

USER jovyan

# Рабочая директория
WORKDIR /home/jovyan/work