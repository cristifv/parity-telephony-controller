package parity.telephonycontroller.node;

import java.util.regex.Pattern;

/**
 * What TC writes into freeswitch, and the rules that keep it from becoming more than it should
 * (TC ADR 0002 §1, R1-1, R1-6). freeswitch expands {@code ${…}} and {@code $(…)} in {@code set}
 * arguments and dial strings, splits dial-string variables on commas, and runs whatever an
 * {@code api_hangup_hook} says when a channel hangs up: a value from IE or a carrier that reaches
 * any of these is checked here first, and refused if it could do more than name itself.
 */
public final class FsText {

    private FsText() {}

    static final Pattern UUID = Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");
    static final Pattern NUMBER = Pattern.compile("^\\+?[0-9]{1,20}$");
    static final Pattern SIP_URI = Pattern.compile("^sips?:([A-Za-z0-9._+-]{1,64})@([A-Za-z0-9.-]{1,253})(:[0-9]{1,5})?$");
    static final Pattern HOST_PORT = Pattern.compile("^[A-Za-z0-9.-]{1,253}(:[0-9]{1,5})?$");
    static final Pattern NAME = Pattern.compile("^[A-Za-z0-9._:-]{1,128}$");

    /**
     * The SIP headers an agent's page trusts (TC-031, the gateway plan's ruling Q4): it auto-answers
     * only a reserved call whose interaction id matches. TC alone writes them, on agent legs; a
     * carrier's or an endpoint's copies never reach an agent (freeswitch/conf: removed on arrival,
     * and {@code sip_copy_custom_headers=false} on every channel that bridges).
     */
    public static final String INTERACTION_HEADER = "X-Parity-Interaction-Id";
    public static final String LEG_HEADER = "X-Parity-Leg-Id";

    /** A value that would be more than text to freeswitch: refused, the message naming the rule only. */
    public static final class Refused extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        Refused(@com.google.errorprone.annotations.CompileTimeConstant String rule) {
            super(rule);
        }
    }

    /**
     * TC's token for one session's legs (R1-1): a UUID, so a hangup hook naming it can match nothing
     * but that session's legs. There is no way to make one from an empty or other string.
     */
    public record SessionToken(String value) {
        public SessionToken {
            if (value == null || !UUID.matcher(value).matches()) {
                throw new Refused("a session token is a lowercase UUID");
            }
        }
    }

    /**
     * The customer leg's hangup coupling (R1-1): {@code hupall} on the session's token. Built only
     * from a {@link SessionToken}: {@code hupall} with an empty value would hang up every channel on
     * the node ({@code mod_commands.c:6694-6707}).
     */
    public record HangupHook(SessionToken token) {
        /**
         * The cause the hook's hangups carry, so the real node can tell them from a far end's or
         * freeswitch's own: one freeswitch never raises on these paths (not ORIGINATOR_CANCEL, which
         * an originate uses when its caller goes).
         */
        public static final String CAUSE = "EXCHANGE_ROUTING_ERROR";

        public HangupHook {
            if (token == null) {
                throw new Refused("a hangup hook needs a session token");
            }
        }

        public String value() {
            return "hupall " + CAUSE + " cc_session_token " + token.value();
        }
    }

    /** A uuid as freeswitch prints them. */
    public static String uuid(String s) {
        if (s == null || !UUID.matcher(s).matches()) {
            throw new Refused("not a lowercase UUID");
        }
        return s;
    }

    /** An E.164-ish number. */
    public static String number(String s) {
        if (s == null || !NUMBER.matcher(s).matches()) {
            throw new Refused("not a phone number");
        }
        return s;
    }

    /** A tenant, bin, replica or trunk name. */
    public static String name(String s) {
        if (s == null || !NAME.matcher(s).matches()) {
            throw new Refused("not a name");
        }
        return s;
    }

    /**
     * A value freeswitch will expand or split (R1-6): no {@code $}, no {@code (}, none of
     * {@code { } [ ] ' ^ ,}, no line break or NUL.
     */
    public static String expandSafe(String s) {
        if (s == null) {
            throw new Refused("a null value");
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '$' || c == '(' || c == '{' || c == '}' || c == '[' || c == ']' || c == '\'' || c == '^'
                    || c == ',' || c == '\r' || c == '\n' || c == 0) {
                throw new Refused("a character freeswitch would expand or split");
            }
        }
        return s;
    }

    /** An SDP TC hands freeswitch in a {@code set} body: no expansion characters. */
    public static String sdp(String s) {
        if (s == null || s.isEmpty() || s.indexOf('$') >= 0 || s.length() > 16_384) {
            throw new Refused("an SDP freeswitch would expand, or none");
        }
        return s;
    }

    /** {@code {a=b,c='d e'}endpoint}: variables are TC's names; values checked, or quoted when TC's own. */
    public static final class DialString {
        private final StringBuilder vars = new StringBuilder();

        /** A value that may come from IE or a carrier: checked by {@link #expandSafe}. */
        public DialString var(String name, String value) {
            return raw(name, expandSafe(value));
        }

        /**
         * One of TC's own values (it may hold spaces, commas and {@code =}, never {@code $}): single
         * quotes, commas escaped.
         */
        public DialString own(String name, String value) {
            if (value.indexOf('$') >= 0 || value.indexOf('\'') >= 0 || value.indexOf('\n') >= 0) {
                throw new Refused("TC's own value can't hold $, a quote or a line break");
            }
            return raw(name, "'" + value.replace(",", "\\,") + "'");
        }

        /**
         * {@link #INTERACTION_HEADER} and {@link #LEG_HEADER} on the dialled leg's INVITE (TC-031):
         * the session's interaction id and TC's leg id, each a lowercase UUID or refused.
         */
        public DialString parityHeaders(String interactionId, String legId) {
            raw("sip_h_" + INTERACTION_HEADER, uuid(interactionId));
            return raw("sip_h_" + LEG_HEADER, uuid(legId));
        }

        private DialString raw(String name, String value) {
            if (!name.matches("^[a-z0-9_]{1,64}$") && !name.matches("^sip_h_[A-Za-z0-9-]{1,64}$")) {
                throw new Refused("a variable name outside [a-z0-9_]");
            }
            if (vars.length() > 0) {
                vars.append(',');
            }
            vars.append(name).append('=').append(value);
            return this;
        }

        public String build(String endpoint) {
            return "{" + vars + "}" + endpoint;
        }
    }
}
