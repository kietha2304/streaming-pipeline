FROM apache/spark:3.5.3

USER root

# Bake the jars into the image instead of --packages, so submits don't download them each time.
# hadoop-aws must match Spark's Hadoop exactly (3.3.4); 1.12.262 is the AWS SDK it was built with.
RUN cd /opt/spark/jars \
 && for url in \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar; \
    do wget -q "$url"; done

# Python API for Delta (DeltaTable). --no-deps: PySpark already ships with the image.
RUN pip install --no-cache-dir --no-deps delta-spark==3.2.1

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
