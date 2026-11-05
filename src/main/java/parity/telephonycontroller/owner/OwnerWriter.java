package parity.telephonycontroller.owner;

/**
 * What a script does to IE's ownership records (tests and the fake only: TC never writes etcd).
 * {@link #claim} returns the record's real create revision, which the runners map the script's
 * epoch onto.
 */
public interface OwnerWriter {

    /** Creates {@code ie/owner/{interactionId}}; {@code epoch} is the script's number for it. */
    long claim(String interactionId, long epoch, OwnerRecord value);

    void delete(String interactionId);

    void register(String bin, String replicaId);

    /** etcd down ({@code false}) or back. */
    void setAvailable(boolean up);

    /** Forgets everything (a script's reset). */
    void clear();
}
