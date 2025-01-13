FROM apache/airflow:2.10.4-python3.12

USER root

# Install OpenJDK and other dependencies
RUN apt-get update && \
    apt-get install -y \
    openjdk-17-jdk \
    postgresql-client \
    netcat-openbsd \
    wget \
    && apt-get clean

# Download and set up Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set up environment variables with explicit Java path
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:/home/airflow/.local/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

# Create data directories with correct ownership
RUN mkdir -p /opt/airflow/data/raw /opt/airflow/data/processed && \
    chown -R 50000:0 /opt/airflow/data /opt/spark

USER airflow
COPY requirements.txt /opt/airflow/
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

WORKDIR /opt/airflow

# Copy and set up initialization script
COPY --chown=50000:0 init.sh /init.sh
RUN chmod +x /init.sh

ENTRYPOINT ["/init.sh"]