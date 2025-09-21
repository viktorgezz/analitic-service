FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /app

COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

COPY --from=build /app/target/analytic-service-0.0.1-SNAPSHOT.jar analytic-service.jar

EXPOSE 8085

CMD ["java", "-jar", "analytic-service.jar"]