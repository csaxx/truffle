# Truffle – Flink 2.2.0 runtime image with GraalVM JDK 21
#
# No application JAR is baked in.  The FlinkDeployment spec fetches the fat JAR
# from Nexus at pod start-up via spec.job.jarURI, e.g.:
#   jarURI: http://nexus.internal/repository/releases/org/csa/truffle/truffle/1.0/truffle-fat-1.0.jar
#
# Required images in the local mirror (set --build-arg REGISTRY=<host:port>):
#   ${REGISTRY}/ghcr.io/graalvm/jdk-community:21
#   ${REGISTRY}/apache/flink:2.2.0-java21
#
# Build:
#   docker build --build-arg REGISTRY=myregistry.local:5000 -t truffle-flink:2.2.0 .

ARG REGISTRY=myregistry.local:5000
ARG GRAALVM_TAG=21

# ── Stage 1: normalise GraalVM path ──────────────────────────────────────────
# The path under JAVA_HOME differs between image variants:
#   RPM (Oracle Linux)  →  /usr/lib64/graalvm/graalvm-community-java21/
#   DEB (Ubuntu)        →  /opt/graalvm-community-openjdk-21.*/
# Copying to /opt/graalvm gives Stage 2 a stable, version-independent path.
FROM ${REGISTRY}/ghcr.io/graalvm/jdk-community:${GRAALVM_TAG} AS graalvm
RUN cp -a "${JAVA_HOME}" /opt/graalvm

# ── Stage 2: Flink 2.2.0 + GraalVM runtime ───────────────────────────────────
FROM ${REGISTRY}/apache/flink:2.2.0-java21

# Replace the OpenJDK bundled in the Flink base image with GraalVM so the
# Truffle JIT can compile GraalPy (python-community) bytecode at runtime.
# Both Flink and the Flink Kubernetes Operator honour the JAVA_HOME env var.
COPY --from=graalvm /opt/graalvm /opt/graalvm/

ENV JAVA_HOME=/opt/graalvm
ENV PATH=/opt/graalvm/bin:${PATH}

USER flink
