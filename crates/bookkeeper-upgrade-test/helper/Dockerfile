# Sidecar image for the Java BK workload driver used by the upgrade test.
#
# Two stages:
#   1. `builder`: use an image with a JDK to compile BkUpgradeHelper.java
#      against the BookKeeper client jars (pulled from the runtime image).
#   2. `runtime`: the apache/bookkeeper image + the compiled .class files.
#
# Resulting image: `bk-upgrade-helper:latest`. The Rust test builds it once
# at startup (or reuses it if already present).

FROM apache/bookkeeper:4.17.3 AS bklibs

FROM eclipse-temurin:17-jdk AS builder
WORKDIR /build
COPY --from=bklibs /opt/bookkeeper/lib /opt/bookkeeper/lib
COPY BkUpgradeHelper.java /build/
RUN javac -cp "/opt/bookkeeper/lib/*" -d /build/classes BkUpgradeHelper.java

FROM apache/bookkeeper:4.17.3
COPY --from=builder /build/classes /helper-classes
ENTRYPOINT ["java", "-cp", "/opt/bookkeeper/lib/*:/helper-classes", "BkUpgradeHelper"]
