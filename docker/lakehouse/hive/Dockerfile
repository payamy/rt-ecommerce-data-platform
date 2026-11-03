# Use the upstream image only as a source of Java distributions; no AMD64
# commands run in this stage. The service runs on the selected target platform JVM below.
FROM --platform=linux/amd64 apache/hive:3.1.3 AS distributions

FROM eclipse-temurin:8-jre-jammy
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1000 hive
ENV HADOOP_HOME=/opt/hadoop \
    HIVE_HOME=/opt/hive \
    HIVE_CONF_DIR=/opt/hive/conf \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PATH=/opt/hive/bin:/opt/hadoop/bin:$PATH
COPY --from=distributions --chown=hive:hive /opt/hadoop /opt/hadoop
COPY --from=distributions --chown=hive:hive /opt/hive /opt/hive
# Hadoop's optional native libraries in the source distribution are AMD64.
# Metastore filesystem operations use the portable Java implementation.
RUN rm -rf /opt/hadoop/lib/native
ADD https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar /opt/hive/lib/postgres.jar
RUN chmod 644 /opt/hive/lib/postgres.jar
COPY metastore.sh /opt/hive/metastore.sh
USER hive
WORKDIR /opt/hive
ENTRYPOINT ["bash", "/opt/hive/metastore.sh"]
