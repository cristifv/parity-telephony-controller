package parity.telephonycontroller.session;

/** One replica's Watch stream, as the engine sees it. Sending never blocks. */
public interface StreamSink {

    /** Queues a message; false once the stream is gone (closed, dropped, or its reader fell behind). */
    boolean send(Events.Delivery d);

    /** True when everything sent so far has gone out and the stream can take more. */
    boolean drained();

    /** Ends the stream with an error (superseded, or the controller restarting). */
    void close(Code code);

    /** The connection dropped (a script's {@code stream_drop}): UNAVAILABLE, no trailer. */
    void drop();
}
