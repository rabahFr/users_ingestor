FROM python:3.8.10-alpine

ARG SPARK_VERSION=3.2.1
ARG HADOOP_VERSION_SHORT=3.2
ARG HADOOP_VERSION=3.2.0
ARG AWS_SDK_VERSION=1.11.375

RUN apk add --no-cache bash openjdk8-jre && \
  apk add --no-cache libc6-compat && \
  ln -s /lib/libc.musl-x86_64.so.1 /lib/ld-linux-x86-64.so.2 && \
  pip install findspark

# Download and extract Spark
RUN wget -qO- https://archive.apache.org/dist/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz | tar zx -C /opt && \
    mv /opt/spark-3.2.4-bin-hadoop3.2.tgz /opt/spark

# Add hadoop-aws and aws-sdk
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.4//hadoop-aws-3.2.4.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar -P /opt/spark/jars/

ENV PATH="/opt/spark/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:${PYTHONPATH}"

# Copy the entire repository to a directory in the Docker image
COPY . /app

# Set the working directory to the copied directory
WORKDIR /app

# Install Python packages specified in requirements.txt
RUN pip install -r requirements.txt