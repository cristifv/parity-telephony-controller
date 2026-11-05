package parity.telephonycontroller.owner;

import java.util.Optional;

/** IE's ownership records, read-only: TC writes nothing to etcd (TC ADR 0001 §4.3). */
public interface OwnerDirectory {

    /** The owner key of an interaction, by a linearizable read; empty if there is none. */
    Optional<OwnerRecord> owner(String interactionId) throws OwnerDirectoryUnavailable;

    /** Whether {@code ie/replicas/{bin}/{replicaId}} exists (checked when a stream opens). */
    boolean replicaRegistered(String bin, String replicaId) throws OwnerDirectoryUnavailable;
}
