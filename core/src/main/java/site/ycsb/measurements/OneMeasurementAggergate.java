
package site.ycsb.measurements;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.*;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import site.ycsb.measurements.exporter.*;

/**
 * Measurement Counter.
 */
public class OneMeasurementAggergate extends MultiMeasurement {

  private final ConcurrentHashMap<String, ConcurrentLinkedQueue<Integer>> measurements;
  private final Properties props;

  public OneMeasurementAggergate(String name, Properties props) {
    super(name);
    measurements = new ConcurrentHashMap<>();
    this.props = props;
  }

  @Override
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException {

    String exportFile = props.getProperty("aggregate_file");
    String exportType = props.getProperty("aggregate_export_type", "csv");
    if (exportFile == null) {
      return;
    }

    System.out.println("Writing aggregate_file, total keys: " + measurements.size());
    if (exportType.equals("csv")) {

      FileOutputStream out = new FileOutputStream(exportFile);
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
      bw.write("QueryType,Microseconds");
      bw.newLine();
      for (Entry<String, ConcurrentLinkedQueue<Integer>> entry : measurements.entrySet()) {
        for (int v : entry.getValue()) {
          bw.write(entry.getKey().replace("aggregate_", "") + "," + v);
          bw.newLine();
        }
      }

      bw.close();

      return;

    }

    if (exportType.equals("json")) {

      FileOutputStream out = new FileOutputStream(exportFile);
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
      JsonFactory factory = new JsonFactory();

      JsonGenerator g = factory.createJsonGenerator(bw);
      g.setPrettyPrinter(new DefaultPrettyPrinter());

      g.writeStartObject();
      for (Entry<String, ConcurrentLinkedQueue<Integer>> entry : measurements.entrySet()) {
        g.writeArrayFieldStart(entry.getKey());
        for (int v : entry.getValue()) {
          g.writeNumber(v);
        }
        g.writeEndArray();

      }

      g.writeEndObject();

      g.close();
    }
  }

  @Override
  public String getSummary() {
    StringBuilder sb = new StringBuilder();
    sb.append("op:" + getName());
    return sb.toString();
  }

  @Override
  public void measure(int count) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void measure(String label, int latency) {
    if (!measurements.containsKey(label)) {
      measurements.putIfAbsent(label, new ConcurrentLinkedQueue<>());
    }

    measurements.get(label).add(latency);

  }
}
