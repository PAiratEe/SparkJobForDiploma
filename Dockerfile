FROM apache/spark:3.5.0

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin
WORKDIR /opt/spark

RUN mkdir -p /tmp/spark_ge_batches

RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar && \
    curl -L -o /opt/spark/jars/clickhouse-jdbc-0.4.6.jar https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6.jar && \
    curl -L -o /opt/spark/jars/acryl-spark-lineage_2.12-0.2.18.jar https://repo1.maven.org/maven2/io/acryl/acryl-spark-lineage_2.12/0.2.18/acryl-spark-lineage_2.12-0.2.18.jar


COPY target/SparkJob-1.0-SNAPSHOT.jar /opt/spark/work-dir/
