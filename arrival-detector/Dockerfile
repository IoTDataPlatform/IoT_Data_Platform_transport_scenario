FROM eclipse-temurin:21-jdk AS build

WORKDIR /app

COPY build/libs/*-all.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]
