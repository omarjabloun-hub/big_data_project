# Stage 1: Build the Java project using Maven
FROM maven:3.6.3-jdk-11 as builder
WORKDIR /app

# Copy the Java project into the Docker image
COPY ./batch_processing .

# List contents of /app to verify the copy worked
RUN ls -la /app

# Attempt to build the project with Maven
RUN mvn -f /app/pom.xml clean package

# Assuming the JAR is correctly built, specify the JAR file explicitly
COPY /batch_processing/target/batch_processing-1.0-SNAPSHOT.jar /app/batch.jar

# Stage 2: Setup the Hadoop image from the custom base
FROM liliasfaxi/hadoop-cluster:latest

# RUN SSH
RUN service ssh start

# Copy the built JAR from the builder stage into the Hadoop container
COPY --from=builder /app/batch.jar /root/batch.jar

# Copy the initialization script and dataset into the Hadoop container
COPY ./dataset/customer_data.csv /root/customer_data.csv

# Run the initialization script upon container startup
CMD ["/root/init-hadoop.sh"]
