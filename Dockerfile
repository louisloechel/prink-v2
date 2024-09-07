FROM maven as build
WORKDIR /app
COPY pom.xml .
COPY src ./src
# Build the application using Maven
RUN mvn clean package -DskipTests

FROM flink

COPY --from=build /app/target/prink-*.jar /opt/flink/usrlib/artifacts/1/

CMD [ "--job-classname", "ganges.GangesEvaluation" ]