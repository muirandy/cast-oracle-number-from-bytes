####
# This Dockerfile is used in order to build a container that runs the Quarkus application in JVM mode
#
# Before building the docker image run:
#
# mvn package
#
# Then, build the image with:
#
# docker build -f src/main/docker/jvm/Dockerfile -t quarkus/cast-oracle-number-from-bytes-jvm .
#
# Then run the container using:
#
# docker run -i --rm -p 8080:8080 quarkus/cast-oracle-number-from-bytes-jvm
#
###
FROM fabric8/java-centos-openjdk8-jdk
ENV JAVA_OPTIONS="-Dquarkus.http.host=0.0.0.0 -Djava.util.logging.manager=org.jboss.logmanager.LogManager"
ENV AB_ENABLED=jmx_exporter

USER root
RUN yum -y update
RUN yum -y install wget

ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/${DOCKERIZE_VERSION}/dockerize-linux-amd64-${DOCKERIZE_VERSION}.tar.gz
RUN tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz
RUN rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz
USER jboss

COPY target/lib/* /deployments/lib/
COPY target/*-runner.jar /deployments/app.jar
EXPOSE 8080

# run with user 1001 and be prepared for be running in OpenShift too
#RUN adduser -G root --no-create-home --disabled-password 1001 \
#  && chown -R 1001 /deployments \
#  && chmod -R "g+rwX" /deployments \
#  && chown -R 1001:root /deployments
#USER 1001

CMD ["/deployments/run-java.sh", "-Dquarkus.http.host=0.0.0.0"]
#ENTRYPOINT [ "/deployments/run-java.sh" ]