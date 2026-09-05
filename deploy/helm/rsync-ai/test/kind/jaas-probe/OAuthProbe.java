import java.util.*;
import javax.security.auth.callback.*;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslClientCallbackHandler;

public class OAuthProbe {

    static final String MODULE =
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";

    static List<javax.security.auth.login.AppConfigurationEntry> entries(String jaas) {
        Map<String, Object> c = new HashMap<>();
        c.put("sasl.jaas.config",
              new org.apache.kafka.common.config.types.Password(jaas));
        return JaasContext.loadClientContext(c).configurationEntries();
    }

    /** Which handler classes exist in this kafka-clients build? */
    static void probeClasses() {
        String[] names = {
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler",
            "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler",
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler",
            "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerValidatorCallbackHandler",
        };
        System.out.println("== handler classes present ==");
        for (String n : names) {
            String r;
            try { Class.forName(n); r = "PRESENT"; }
            catch (Throwable t) { r = "ABSENT"; }
            System.out.printf("  %-8s %s%n", r, n);
        }
        System.out.println();
    }

    static AuthenticateCallbackHandler newHandler() throws Exception {
        return (AuthenticateCallbackHandler) Class
            .forName("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler")
            .getDeclaredConstructor().newInstance();
    }

    /** configure() the login handler and report what it accepts / complains about. */
    static void probeConfigure(String label, String jaas, boolean withEndpoint) {
        try {
            Map<String, Object> configs = new HashMap<>();
            // Kafka normally merges these in from SaslConfigs defaults; a bare map
            // omits them, so supply them or every case fails on the first one and
            // the credential question stays unanswered.
            configs.put("sasl.login.retry.backoff.ms", 100L);
            configs.put("sasl.login.retry.backoff.max.ms", 10000L);
            configs.put("sasl.login.connect.timeout.ms", 5000);
            configs.put("sasl.login.read.timeout.ms", 5000);
            // Claim-name defaults, consumed at OAuthBearerLoginCallbackHandler:190 --
            // a *later* step than the credential read at :189, and nothing to do with
            // the JAAS `scope` option. Without them every case dies at :190 and the
            // credential question never gets answered.
            configs.put("sasl.oauthbearer.scope.claim.name", "scope");
            configs.put("sasl.oauthbearer.sub.claim.name", "sub");
            if (withEndpoint)
                configs.put("sasl.oauthbearer.token.endpoint.url", "http://127.0.0.1:1/token");
            AuthenticateCallbackHandler h = newHandler();
            h.configure(configs, "OAUTHBEARER", entries(jaas));
            System.out.printf("  %-38s configure=OK%n", label);
            h.close();
        } catch (Throwable t) {
            Throwable r = t; while (r.getCause() != null) r = r.getCause();
            String m = String.valueOf(r.getMessage()).replaceAll("\\s+", " ");
            if (m.length() > 150) m = m.substring(0, 150) + "...";
            System.out.printf("  %-38s %s: %s%n", label,
                              r.getClass().getSimpleName(), m);
            if (System.getenv("TRACE") != null) {
                for (StackTraceElement e : r.getStackTrace()) {
                    String s = e.toString();
                    if (s.startsWith("org.apache.kafka")) System.out.println("        at " + s);
                }
            }
        }
    }

    /** Do `extension_*` JAAS options reach the wire as SASL extensions? */
    static void probeExtensions(String jaas) {
        System.out.println("== extension_ JAAS options ==");
        try {
            OAuthBearerSaslClientCallbackHandler h = new OAuthBearerSaslClientCallbackHandler();
            h.configure(new HashMap<>(), "OAUTHBEARER", entries(jaas));
            System.out.println("  client callback handler configure=OK (reads extension_* from JAAS)");
        } catch (Throwable t) {
            Throwable r = t; while (r.getCause() != null) r = r.getCause();
            System.out.println("  " + r.getClass().getSimpleName() + ": " + r.getMessage());
        }
        // Show that JaasContext itself surfaces the options verbatim.
        Map<String, ?> opts = entries(jaas).get(0).getOptions();
        List<String> keys = new ArrayList<>(opts.keySet());
        Collections.sort(keys);
        System.out.println("  JAAS options seen: " + keys);
        System.out.println();
    }

    public static void main(String[] args) throws Exception {
        probeClasses();

        System.out.println("== OAuthBearerLoginCallbackHandler.configure() ==");
        probeConfigure("full (id+secret+scope)+endpoint",
            MODULE + " required clientId=\"cid\" clientSecret=\"csec\" scope=\"sc\";", true);
        probeConfigure("id+secret, no scope, +endpoint",
            MODULE + " required clientId=\"cid\" clientSecret=\"csec\";", true);
        probeConfigure("NO clientId (control) +endpoint",
            MODULE + " required clientSecret=\"csec\";", true);
        probeConfigure("NO clientSecret (control) +endpoint",
            MODULE + " required clientId=\"cid\";", true);
        probeConfigure("bare 'required;' (control) +endpoint",
            MODULE + " required;", true);
        probeConfigure("full but NO endpoint (control)",
            MODULE + " required clientId=\"cid\" clientSecret=\"csec\";", false);
        System.out.println();

        probeExtensions(MODULE
            + " required clientId=\"cid\" clientSecret=\"csec\" extension_logicalCluster=\"lkc-1\";");
    }
}
