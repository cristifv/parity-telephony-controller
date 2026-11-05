package parity.telephonycontroller.obs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Personal data in logs (ARCHITECTURE.md §5.2): the same rules as interaction-engine's
 * {@code ie-obs} ({@code crates/ie-obs/src/pii.rs}), checked by both against parity-contracts'
 * {@code masking/cases.v1.json}, plus {@code masking/billing.v1.json}'s names (log-record.v1:
 * "Personal data in logs").
 * <ul>
 *   <li>{@link #normalize}: camelCase split ({@code callerNumber} → {@code caller_number},
 *       {@code DTMFDigit} → {@code dtmf_digit}), ASCII lowercase, {@code -} → {@code _}, a
 *       leading {@code variable_} (freeswitch channel variables) stripped;</li>
 *   <li>{@link #classify}: phone (masked to its last 3 digits), secret ({@code [redacted]}),
 *       email ({@code [email]}) or plain, by the normalized field name, whatever the value's
 *       type;</li>
 *   <li>{@link #maskField}: the policy applied to a value, at every depth;</li>
 *   <li>{@link #maskText} and {@link #maskProse}: free text (a plain field's string, a message,
 *       an exception's text) loses any E.164 number, {@code sip:}/{@code tel:} user part of 7 or
 *       more digits and email address (contract-check's free-text scan), and prose also any run
 *       of 7 or more digits.</li>
 * </ul>
 *
 * <p><b>Exception messages carry no data:</b> no phone number, digit, SDP, key or other personal
 * value, because they reach logs unmasked (the formatter masks fields, not messages). They may
 * name configuration (a property, a path, a {@code host:port}).
 */
public final class Pii {

    /** What the formatter does with a field. */
    public enum FieldClass { PHONE, SECRET, EMAIL, PLAIN }

    public static final String REDACTED = "[redacted]";
    public static final String EMAIL = "[email]";

    private static final List<String> PHONE_NAMES = List.of(
            "from", "to", "number", "phone", "caller", "callee", "caller_id", "callback_number",
            "destination", "ani", "dnis", "msisdn", "e164", "contact", "uri", "pai",
            "p_asserted_identity", "rpid", "remote_party_id", "diversion", "history_info", "user",
            // freeswitch ESL names that carry numbers (ie-obs F-C).
            "channel_name", "caller_channel_name", "caller_username", "rdnis", "dialed_extension",
            "originate_string", "dial_string", "sip_from_display", "caller_caller_id_name",
            "sip_destination_url", "sip_call_id");
    private static final List<String> SECRET_NAMES = List.of(
            "digits", "dtmf", "pin", "password", "secret", "token", "authorization", "api_key", "dek",
            "wrapped_dek", "sdp", "sip_body", "passwd", "credentials",
            // masking/billing.v1.json's secrets the rules above don't catch.
            "stripe_signature", "cf_turnstile_response", "checkout_url", "portal_url", "verification_url",
            "secret_key");
    /** Any name with one of these {@code _}-separated tokens is a secret (DTMF digits: PCI). */
    private static final Set<String> SECRET_TOKENS = Set.of("dtmf", "digit", "digits");
    /** Names a rule would catch that are known not to hold personal data. */
    private static final Set<String> EXEMPT = Set.of("reply_to", "num_digits");

    private Pii() {}

    /**
     * The name the policy sees (parity-contracts' {@code masking/cases.v1.json}, "normalize"):
     * <ol>
     *   <li>an {@code _} goes before an ASCII uppercase letter that follows an ASCII lowercase
     *       letter or an ASCII digit, or that follows an ASCII uppercase letter and is followed
     *       by an ASCII lowercase letter ({@code callerNumber} → {@code caller_Number},
     *       {@code DTMFDigit} → {@code DTMF_Digit}, {@code Caller-ID} unchanged);</li>
     *   <li>ASCII uppercase → lowercase; {@code -} → {@code _};</li>
     *   <li>a leading {@code variable_} is stripped.</li>
     * </ol>
     */
    public static String normalize(String name) {
        StringBuilder b = new StringBuilder(name.length() + 4);
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (upper(c) && i > 0) {
                char prev = name.charAt(i - 1);
                boolean next = i + 1 < name.length() && lower(name.charAt(i + 1));
                if (lower(prev) || asciiDigit(prev) || (upper(prev) && next)) {
                    b.append('_');
                }
            }
            if (upper(c)) {
                b.append((char) (c + ('a' - 'A')));
            } else if (c == '-') {
                b.append('_');
            } else {
                b.append(c);
            }
        }
        String lower = b.toString();
        return lower.startsWith("variable_") ? lower.substring("variable_".length()) : lower;
    }

    /** {@code name} equals {@code listed}, or ends with {@code _} + {@code listed}. */
    private static boolean matches(String name, String listed) {
        return name.equals(listed)
                || (name.length() > listed.length()
                        && name.endsWith(listed)
                        && name.charAt(name.length() - listed.length() - 1) == '_');
    }

    /** The policy for a field name (normalized first). */
    public static FieldClass classify(String name) {
        String n = normalize(name);
        if (EXEMPT.contains(n)) {
            return FieldClass.PLAIN;
        }
        if (n.equals("email") || n.endsWith("_email")) {
            return FieldClass.EMAIL;
        }
        for (String token : n.split("_", -1)) {
            if (SECRET_TOKENS.contains(token)) {
                return FieldClass.SECRET;
            }
        }
        if (SECRET_NAMES.stream().anyMatch(s -> matches(n, s))
                || n.endsWith("_secret") || n.endsWith("_token") || n.endsWith("_password")) {
            return FieldClass.SECRET;
        }
        if (PHONE_NAMES.stream().anyMatch(p -> matches(n, p))
                || n.endsWith("_number") || n.endsWith("_phone") || n.endsWith("_user")) {
            return FieldClass.PHONE;
        }
        return FieldClass.PLAIN;
    }

    private static boolean asciiDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean upper(char c) {
        return c >= 'A' && c <= 'Z';
    }

    private static boolean lower(char c) {
        return c >= 'a' && c <= 'z';
    }

    /**
     * Keeps the last 3 ASCII digits behind {@code ***} when there are at least 4; otherwise
     * {@code ***}. A value already masked ({@code ***123}, {@code Phone(***123)}) keeps its
     * last 3 digits rather than being masked again.
     */
    public static String maskPhone(String raw) {
        StringBuilder digits = new StringBuilder();
        for (int i = 0; i < raw.length(); i++) {
            if (asciiDigit(raw.charAt(i))) {
                digits.append(raw.charAt(i));
            }
        }
        int at = raw.indexOf("***");
        if (at >= 0) {
            StringBuilder tail = new StringBuilder();
            for (int i = at + 3; i < raw.length() && asciiDigit(raw.charAt(i)); i++) {
                tail.append(raw.charAt(i));
            }
            if (tail.length() > 0 && tail.length() <= 3 && tail.length() == digits.length()) {
                return "***" + tail;
            }
        }
        if (digits.length() >= 4) {
            return "***" + digits.substring(digits.length() - 3);
        }
        return "***";
    }

    /** Applies the policy to a value logged under {@code name}. */
    public static Object maskField(String name, Object value) {
        return switch (classify(name)) {
            case PLAIN -> maskNested(value);
            case SECRET -> REDACTED;
            case EMAIL -> EMAIL;
            case PHONE -> maskAll(value);
        };
    }

    /** Recurses into maps and collections, applying the policy to every key. */
    public static Object maskNested(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            map.forEach((k, v) -> out.put(String.valueOf(k), maskField(String.valueOf(k), v)));
            return out;
        }
        if (value instanceof Collection<?> items) {
            List<Object> out = new ArrayList<>(items.size());
            items.forEach(item -> out.add(maskNested(item)));
            return out;
        }
        if (value instanceof Phone phone) {
            return phone.toString();
        }
        return value;
    }

    /** Everything under a phone key is masked, scalars and nested values alike. */
    public static Object maskAll(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            map.forEach((k, v) -> out.put(String.valueOf(k), maskAll(v)));
            return out;
        }
        if (value instanceof Collection<?> items) {
            List<Object> out = new ArrayList<>(items.size());
            items.forEach(item -> out.add(maskAll(item)));
            return out;
        }
        if (value instanceof Boolean) {
            return value;
        }
        return maskPhone(String.valueOf(value));
    }

    /**
     * Free text under a plain name, as contract-check scans it: an E.164 number ({@code +} and 7
     * or more digits) and a {@code sip:}/{@code sips:}/{@code tel:} user part of 7 or more digits
     * keep their last 3 digits ({@code ***123}); an email address becomes {@code [email]}.
     */
    public static String maskText(String s) {
        return mask(s, false);
    }

    /**
     * A message or an exception's text: {@link #maskText}, and also any run of 7 or more digits
     * (a number written without its {@code +}).
     */
    public static String maskProse(String s) {
        return mask(s, true);
    }

    private static String mask(String s, boolean bareRuns) {
        if (s == null || s.isEmpty() || !suspicious(s, bareRuns)) {
            return s;
        }
        StringBuilder out = new StringBuilder(s.length());
        int i = 0;
        while (i < s.length()) {
            int run = digitRunAfter(s, i, bareRuns);
            if (run > 0) {
                int end = i + run;
                int start = s.charAt(i) == '+' ? i + 1 : i;
                out.append("***");
                // Before an '@' the 3 digits would read as an email's local part: none kept.
                if (end >= s.length() || s.charAt(end) != '@') {
                    out.append(s, Math.max(start, end - 3), end);
                }
                i = end;
                continue;
            }
            int email = emailAt(s, i, out);
            if (email > 0) {
                i = email;
                continue;
            }
            out.append(s.charAt(i));
            i++;
        }
        return out.toString();
    }

    /** Anything worth a closer look: a {@code +}, an {@code @}, a {@code :}, or (prose) 7 digits. */
    private static boolean suspicious(String s, boolean bareRuns) {
        int digits = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '+' || c == '@' || c == ':') {
                return true;
            }
            digits = asciiDigit(c) ? digits + 1 : 0;
            if (bareRuns && digits >= 7) {
                return true;
            }
        }
        return false;
    }

    /**
     * The length of a number to mask that starts at {@code i}: {@code +} and 7 or more digits (the
     * first not 0); 7 or more digits right after {@code sip:}, {@code sips:}, {@code tel:} or their
     * {@code +}; with {@code bareRuns}, any 7 or more digits not preceded by a digit. 0 otherwise.
     */
    private static int digitRunAfter(String s, int i, boolean bareRuns) {
        char c = s.charAt(i);
        if (c == '+') {
            int n = digits(s, i + 1);
            boolean afterScheme = schemeEndsAt(s, i);
            if (n >= 7 && (afterScheme || s.charAt(i + 1) != '0' || bareRuns)) {
                return n + 1;
            }
            return 0;
        }
        if (!asciiDigit(c) || (i > 0 && asciiDigit(s.charAt(i - 1)))) {
            return 0;
        }
        int n = digits(s, i);
        if (n >= 7 && (bareRuns || schemeEndsAt(s, i))) {
            return n;
        }
        return 0;
    }

    private static int digits(String s, int from) {
        int n = 0;
        while (from + n < s.length() && asciiDigit(s.charAt(from + n))) {
            n++;
        }
        return n;
    }

    /** {@code sip:}, {@code sips:} or {@code tel:} (any case) ends right before {@code i}. */
    private static boolean schemeEndsAt(String s, int i) {
        for (String scheme : new String[] {"sip:", "sips:", "tel:"}) {
            int at = i - scheme.length();
            if (at >= 0 && s.regionMatches(true, at, scheme, 0, scheme.length())) {
                return true;
            }
        }
        return false;
    }

    /**
     * An email address whose local part starts at {@code i} (not a SIP URI's user): {@code [email]}
     * is appended to {@code out} and the index after the address returned; else 0.
     */
    private static int emailAt(String s, int i, StringBuilder out) {
        if (!localChar(s.charAt(i)) || (i > 0 && localChar(s.charAt(i - 1)))) {
            return 0;
        }
        int at = i;
        while (at < s.length() && localChar(s.charAt(at))) {
            at++;
        }
        if (at >= s.length() || s.charAt(at) != '@' || schemeEndsAt(s, i)) {
            return 0;
        }
        int end = at + 1;
        while (end < s.length() && (asciiAlnum(s.charAt(end)) || s.charAt(end) == '.' || s.charAt(end) == '-')) {
            end++;
        }
        String domain = s.substring(at + 1, end);
        while (domain.endsWith(".")) {
            domain = domain.substring(0, domain.length() - 1);
        }
        int dot = domain.lastIndexOf('.');
        if (dot <= 0 || domain.length() - dot - 1 < 2) {
            return 0;
        }
        for (int k = dot + 1; k < domain.length(); k++) {
            if (!upper(domain.charAt(k)) && !lower(domain.charAt(k))) {
                return 0;
            }
        }
        out.append(EMAIL);
        return at + 1 + domain.length();
    }

    private static boolean localChar(char c) {
        return asciiAlnum(c) || c == '.' || c == '_' || c == '%' || c == '+' || c == '-';
    }

    private static boolean asciiAlnum(char c) {
        return asciiDigit(c) || upper(c) || lower(c);
    }
}
