FROM debian:jessie

MAINTAINER eduardoalonso <eduardoalonso@stratio.com>

ENV VERSION 7
ENV UPDATE 80
ENV BUILD 15

ENV JAVA_HOME /usr/lib/jvm/java-${VERSION}-oracle

RUN apt-get update && apt-get install ca-certificates curl -y && \
    curl --silent --location --retry 3 --cacert /etc/ssl/certs/GeoTrust_Global_CA.pem \
    --header "Cookie: oraclelicense=accept-securebackup-cookie;" \
    http://download.oracle.com/otn-pub/java/jdk/"${VERSION}"u"${UPDATE}"-b"${BUILD}"/jdk-"${VERSION}"u"${UPDATE}"-linux-x64.tar.gz \
    | tar xz -C /tmp && \
    mkdir -p /usr/lib/jvm && mv /tmp/jdk1.${VERSION}.0_${UPDATE} "${JAVA_HOME}" && \
    apt-get autoclean && apt-get --purge -y autoremove && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN update-alternatives --install "/usr/bin/java" "java" "${JAVA_HOME}/bin/java" 1 && \
    update-alternatives --install "/usr/bin/javaws" "javaws" "${JAVA_HOME}/bin/javaws" 1 && \
    update-alternatives --install "/usr/bin/javac" "javac" "${JAVA_HOME}/bin/javac" 1 && \
    update-alternatives --set java "${JAVA_HOME}/bin/java" && \
    update-alternatives --set javaws "${JAVA_HOME}/bin/javaws" && \
    update-alternatives --set javac "${JAVA_HOME}/bin/javac"


ENV MAVEN_VERSION 3.3.3

RUN curl -fsSL http://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz | tar xzf - -C /usr/share \
  && mv /usr/share/apache-maven-$MAVEN_VERSION /usr/share/maven \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven


RUN apt-get update && apt-get install -y --no-install-recommends git


# Configuration: Cassandra Version & Plugin Version
ENV CASSANDRA_VERSION 2.1.8
ENV PLUGIN_VERSION $CASSANDRA_VERSION.4

RUN apt-key adv --keyserver ha.pool.sks-keyservers.net --recv-keys 514A2AD631A57A16DD0047EC749D6EEC0353B12C

RUN echo 'deb http://www.apache.org/dist/cassandra/debian 21x main' >> /etc/apt/sources.list.d/cassandra.list

RUN apt-get update \
    && apt-get install -y cassandra="$CASSANDRA_VERSION" net-tools unzip python\
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV CASSANDRA_CONFIG /etc/cassandra

# listen to all rpc
# RUN sed -ri ' \
#        s/^(rpc_address:).*/ 0.0.0.0 \1/; \
#   ' "$CASSANDRA_CONFIG/cassandra.yaml"


# Lucene Plugin
RUN cd /tmp/ \
  && git clone https://github.com/Stratio/cassandra-lucene-index \
    && cd cassandra-lucene-index \
    && git checkout $PLUGIN_VERSION \
    && mvn clean package \
    && cp plugin/target/cassandra-lucene-index-plugin-$PLUGIN_VERSION.jar /usr/share/cassandra/lib/

VOLUME /var/lib/cassandra/data
# expose ports 9042=CQL Native Transport Port, 7199=JMX
EXPOSE 9042 7199


# SPARK
ENV SPARK_VERSION 1.4.1
ENV HADOOP_VERSION 2.6
ENV SPARK_PACKAGE $SPARK_VERSION-bin-hadoop$HADOOP_VERSION
ENV SPARK_HOME /usr/spark-$SPARK_PACKAGE
ENV PATH $PATH:$SPARK_HOME/bin
RUN curl -sL --retry 3 "http://mirrors.ibiblio.org/apache/spark/spark-$SPARK_VERSION/spark-$SPARK_PACKAGE.tgz" | gunzip | tar x -C /usr/ && ln -s $SPARK_HOME /usr/spark

COPY docker-entrypoint.sh /docker-entrypoint.sh

RUN mkdir /home/example

COPY spark-2.1.8.4-SNAPSHOT.jar /home/example/spark-2.1.8.4-SNAPSHOT.jar

COPY CreateTableAndPopulate.cql /home/example/

ENTRYPOINT ["/docker-entrypoint.sh"]