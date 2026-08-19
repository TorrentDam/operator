# Build stage
FROM virtuslab/scala-cli:1.16.0 AS builder

WORKDIR /app

# Copy source files
COPY operator.scala .
COPY transmission.scala .
COPY crd.yaml .

# Build executable with Coursier cache mounted as build cache
# This persists dependency downloads across builds without bloating layers
RUN --mount=type=cache,target=/root/.cache/coursier \
    --mount=type=cache,target=/root/.scala-build \
    scala-cli --power package --assembly operator.scala transmission.scala --output operator

# Runtime stage
FROM eclipse-temurin:25-jre

WORKDIR /app

ENV JDK_JAVA_OPTIONS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED"

# Copy the built executable from builder
COPY --from=builder /app/operator .

# Run the operator
ENTRYPOINT ["./operator"]