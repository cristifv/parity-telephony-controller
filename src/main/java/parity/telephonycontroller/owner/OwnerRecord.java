package parity.telephonycontroller.owner;

/**
 * IE's owner key {@code ie/owner/{interaction_id}}: its create revision (the owner epoch) and its
 * value, JSON {@code {"tenant_id", "bin", "replica_id", "node_id"}} (TC ADR 0001 §4.4). A value
 * that doesn't parse leaves the fields null, which no check accepts.
 */
public record OwnerRecord(long createRevision, String tenantId, String bin, String replicaId, String nodeId) {}
