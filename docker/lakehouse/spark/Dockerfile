FROM apache/spark:3.5.7-python3 AS spark-distribution

# Match the Python 3.11 runtime used to develop the processing/lakehouse apps.
# Both stages follow the Compose-selected architecture.
FROM python:3.11-slim-bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless bash procps tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 185 spark
ENV JAVA_HOME=/usr/lib/jvm/default-java \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH
COPY --from=spark-distribution --chown=185:185 /opt/spark /opt/spark
# JDBC sessions must see Delta on Spark's base classpath, including sessions
# created by the Hive Thrift Server rather than the Python Delta builder.
ADD https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar /opt/spark/jars/delta-spark_2.12-3.3.2.jar
ADD https://repo.maven.apache.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar /opt/spark/jars/delta-storage-3.3.2.jar
RUN chmod 644 /opt/spark/jars/delta-*.jar
RUN pip3 install --no-cache-dir --no-deps delta-spark==3.3.2 \
    && pip3 install --no-cache-dir 'importlib-metadata>=1.0'
# Hive's Maven client resolver also uses Java user.home independently of
# spark.jars.ivy. The upstream spark user's home may be /nonexistent.
RUN mkdir -p /home/spark/.ivy2 /tmp/ivy \
    && chown -R 185:0 /home/spark /tmp/ivy
ENV HOME=/home/spark \
    SPARK_SUBMIT_OPTS="-Duser.home=/home/spark"
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/workspace \
    PYSPARK_PYTHON=/usr/local/bin/python3.11 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
WORKDIR /workspace
USER 185
ENTRYPOINT ["/usr/bin/tini", "--"]
