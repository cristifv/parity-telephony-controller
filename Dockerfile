# parity-telephony-controller's image. Step 0: what the node-lock test (NodeLockContainerIT)
# runs; the deployment's image settings (heap, the media node's mounts) come with step 8.
# The JRE: Temurin 21 on Ubuntu noble, pinned by digest (built 2026-09-16; the 14-day rule).
FROM eclipse-temurin:21.0.12_8-jre-noble@sha256:7739f0ffce786528961eea6bf46d9610ee968ac6127c9b2e93494757bdecce9f
COPY app.jar /app/app.jar
# Non-root. The node lock's directory is a host bind mount, /run/parity-telephony-controller.
USER 10001:10001
EXPOSE 9701
# Generational ZGC (PLAN.md "Java design for latency").
ENTRYPOINT ["java", "-XX:+UseZGC", "-XX:+ZGenerational", "-jar", "/app/app.jar"]
