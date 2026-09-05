// Measures what Kafka's own parser does to a SASL password, so the escaping rules
// in this repo are pinned to observed behaviour rather than to a reading of the docs.
//
// Two legs are measured, because values in this chart cross one or both:
//   1. a JAAS string handed straight to sasl.jaas.config       (JAAS grammar only)
//   2. a JAAS string written into a .properties file first     (properties + JAAS)
//
// Leg 2 is the one that bites: java.util.Properties eats a level of backslash before
// the JAAS parser ever sees the value, so escaping once there corrupts identically to
// not escaping at all -- a site can look guarded and not be.
//
// Run with ./run.sh (needs docker; no local JDK required).
import java.io.*;
import java.util.*;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.utils.Utils;

public class Probe {
    static final String MOD = "org.apache.kafka.common.security.plain.PlainLoginModule";

    // The three encodings this repo has shipped at one point or another.
    static String none(String v)      { return v; }
    static String quoteOnly(String v) { return v.replace("\"", "\\\""); }
    static String jaas(String v)      { return v.replace("\\", "\\\\").replace("\"", "\\\""); }
    static String props(String v)     { return v.replace("\\", "\\\\"); }

    static String line(String escaped) {
        return MOD + " required username=\"u\" password=\"" + escaped + "\";";
    }

    static String parse(String jaasLine) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("sasl.jaas.config", new Password(jaasLine));
        try {
            AppConfigurationEntry e = JaasContext.loadClientContext(cfg).configurationEntries().get(0);
            Object pw = e.getOptions().get("password");
            return pw == null ? "<absent>" : pw.toString();
        } catch (Exception ex) {
            return "<ERROR: " + ex.getMessage() + ">";
        }
    }

    static String parseViaPropertiesFile(String jaasLine) throws Exception {
        File f = File.createTempFile("probe", ".properties");
        f.deleteOnExit();
        try (Writer w = new OutputStreamWriter(new FileOutputStream(f))) {
            w.write("sasl.jaas.config=" + jaasLine + "\n");
        }
        return parse(Utils.loadProps(f.getAbsolutePath()).getProperty("sasl.jaas.config"));
    }

    static String show(String s) {
        StringBuilder b = new StringBuilder();
        for (char c : s.toCharArray()) {
            if (c == '\\') b.append("\\\\");
            else if (c == '"') b.append("\\\"");
            else if (c < 32) b.append(String.format("\\x%02x", (int) c));
            else b.append(c);
        }
        return b.toString();
    }

    static String verdict(String got, String want) { return got.equals(want) ? "OK" : show(got); }

    public static void main(String[] a) throws Exception {
        String[] pws = {
            "plain-ok", "pa\\ss", "pa\"ss", "pa\"ss\\x",
            "C:\\Users\\svc", "a\\nb", "tail\\", "sp ace=and&sym",
        };

        System.out.println("LEG 1 -- straight into sasl.jaas.config (config JSON, CONNECT_* env)");
        System.out.printf("%-16s | %-20s | %-20s | %-20s%n",
            "raw password", "no escaping", "quote-only", "jaas escaping");
        System.out.println("-".repeat(84));
        for (String p : pws) {
            System.out.printf("%-16s | %-20s | %-20s | %-20s%n", show(p),
                verdict(parse(line(none(p))), p),
                verdict(parse(line(quoteOnly(p))), p),
                verdict(parse(line(jaas(p))), p));
        }

        System.out.println();
        System.out.println("LEG 2 -- written to a .properties file first (--command-config,");
        System.out.println("         connect-distributed.properties)");
        System.out.printf("%-16s | %-20s | %-20s%n", "raw password", "jaas only", "jaas + properties");
        System.out.println("-".repeat(62));
        for (String p : pws) {
            System.out.printf("%-16s | %-20s | %-20s%n", show(p),
                verdict(parseViaPropertiesFile(line(jaas(p))), p),
                verdict(parseViaPropertiesFile(props(line(jaas(p)))), p));
        }
    }
}
