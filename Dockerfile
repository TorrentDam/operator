# Dependency stage: fetches JVM and dependencies declared in project.scala.
# This layer is cached as long as project.scala doesn't change, so editing
# operator.scala/transmission.scala won't force re-downloading the JVM/deps.
FROM virtuslab/scala-cli:1.16.0 AS deps

WORKDIR /app

COPY project.scala .

RUN scala-cli compile project.scala

# Build stage
FROM virtuslab/scala-cli:1.16.0 AS builder

WORKDIR /app

# Reuse the JVM/dependency caches populated by the deps stage
COPY --from=deps /root/.cache/coursier /root/.cache/coursier
COPY --from=deps /app/.scala-build .scala-build

# Copy source files
COPY project.scala .
COPY operator.scala .
COPY transmission.scala .
COPY crd.yaml .

# Build executable. Directory input (".") is required so project.scala is
# picked up by scala-cli.
RUN scala-cli --power package --assembly . --output operator

# Runtime stage
FROM eclipse-temurin:25-jre

WORKDIR /app

ENV JDK_JAVA_OPTIONS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED"

# Copy the built executable from builder
COPY --from=builder /app/operator .

# Run the operator
ENTRYPOINT ["./operator"]