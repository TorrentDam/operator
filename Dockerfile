# Build stage
FROM sbtscala/scala-sbt:eclipse-temurin-24.0.1_9_1.12.8_3.8.3 AS builder

WORKDIR /app

# Copy source files
COPY operator.scala .
COPY crd.yaml .

# Build executable
RUN scala --power package --assembly operator.scala --output operator

# Runtime stage
FROM eclipse-temurin:25-jre

WORKDIR /app

ENV JDK_JAVA_OPTIONS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED"

# Copy the built executable from builder
COPY --from=builder /app/operator .

# Run the operator
ENTRYPOINT ["./operator"]
