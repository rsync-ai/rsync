// JVM runtime of the Kafka security matrix. Runs inside the kafka-connect image
// with that image's own kafka-clients jar (java -cp '/kafka/libs/*' RoundTrip.java),
// so a PASS is about the client Connect and Debezium actually ship.
//
//   RoundTrip -                  client properties on stdin (what rsync's
//                                Debezium schema-history builder emitted, prefix
//                                stripped). Used for producer and consumer alike.
//   RoundTrip --connect <file>   a live worker's connect-distributed.properties:
//                                producer = bootstrap.servers + producer.*,
//                                consumer = bootstrap.servers + consumer.*,
//                                exactly the split Connect applies to task
//                                clients, which do NOT inherit worker security.
//
// Every security property comes from the input; this program adds only
// serializers and timeouts. It writes PROBE_ID to PROBE_TOPIC (default
// "kmatrix") and reads it back. Output: one line starting with RESULT.
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;

public class RoundTrip {
  public static void main(String[] a) {
    String id = System.getenv("PROBE_ID");
    String topic = Optional.ofNullable(System.getenv("PROBE_TOPIC")).filter(s -> !s.isEmpty()).orElse("kmatrix");
    try {
      Properties pp = new Properties(), cp = new Properties();
      if (a.length == 1 && a[0].equals("-")) {
        Properties in = load(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        pp.putAll(in);
        cp.putAll(in);
      } else if (a.length == 2 && a[0].equals("--connect")) {
        Properties w = load(new FileReader(a[1], StandardCharsets.UTF_8));
        for (String k : w.stringPropertyNames()) {
          if (k.startsWith("producer.")) pp.put(k.substring(9), w.getProperty(k));
          if (k.startsWith("consumer.")) cp.put(k.substring(9), w.getProperty(k));
        }
        pp.put("bootstrap.servers", w.getProperty("bootstrap.servers"));
        cp.put("bootstrap.servers", w.getProperty("bootstrap.servers"));
      } else {
        System.out.println("RESULT FAIL jvm usage: RoundTrip - | RoundTrip --connect <file>");
        System.exit(2);
      }
      pp.put("key.serializer", StringSerializer.class.getName());
      pp.put("value.serializer", StringSerializer.class.getName());
      pp.put("max.block.ms", "15000");
      pp.put("request.timeout.ms", "10000");
      pp.put("delivery.timeout.ms", "20000");
      pp.put("retries", "0");
      RecordMetadata md;
      try (KafkaProducer<String, String> p = new KafkaProducer<>(pp)) {
        md = p.send(new ProducerRecord<>(topic, id)).get();
      }
      cp.put("key.deserializer", StringDeserializer.class.getName());
      cp.put("value.deserializer", StringDeserializer.class.getName());
      cp.put("enable.auto.commit", "false");
      cp.remove("group.id");
      try (KafkaConsumer<String, String> c = new KafkaConsumer<>(cp)) {
        TopicPartition tp = new TopicPartition(topic, md.partition());
        c.assign(List.of(tp));
        c.seek(tp, md.offset());
        long end = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < end) {
          for (ConsumerRecord<String, String> rec : c.poll(Duration.ofSeconds(1)))
            if (id.equals(rec.value())) {
              System.out.println("RESULT PASS jvm round-trip ok");
              System.exit(0);
            }
        }
      }
      System.out.println("RESULT FAIL jvm consume: timed out waiting for own message");
    } catch (Throwable t) {
      StringBuilder sb = new StringBuilder();
      for (Throwable c = t; c != null; c = c.getCause())
        sb.append(sb.length() > 0 ? " <- " : "").append(c.getClass().getSimpleName()).append(": ").append(c.getMessage());
      String s = sb.toString().replaceAll("\\s+", " ");
      System.out.println("RESULT FAIL jvm " + (s.length() > 600 ? s.substring(0, 600) : s));
    }
    System.exit(1);
  }

  private static Properties load(Reader r) throws java.io.IOException {
    Properties p = new Properties();
    try (r) { p.load(r); }
    return p;
  }
}
