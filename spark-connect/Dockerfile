ARG build_for=linux/arm64

FROM --platform=${build_for} bitnami/spark:3.5.0
USER root
RUN install_packages curl
USER 1001
RUN curl https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar --output /opt/bitnami/spark/jars/delta-spark_2.12-3.1.0.jar
# RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar --output /opt/bitnami/spark/jars/hadoop-aws-3.3.6.jar
RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.5.0/spark-connect_2.12-3.5.0.jar --output /opt/bitnami/spark/jars/spark-connect_2.12-3.5.0.jar
RUN curl https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar --output /opt/bitnami/spark/jars/delta-storage-3.1.0.jar
