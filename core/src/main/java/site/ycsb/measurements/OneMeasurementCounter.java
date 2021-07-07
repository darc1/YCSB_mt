
package site.ycsb.measurements;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import site.ycsb.measurements.exporter.*;

/**
 *Measurement Counter.
 * */
public class OneMeasurementCounter extends OneMeasurement {

  private final AtomicInteger counter;

  public OneMeasurementCounter(String name) {
    super(name);
    counter = new AtomicInteger();
  }

  @Override
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
    exporter.write(getName(), getName(), counter.intValue());
  }

  @Override
  public String getSummary() {
    StringBuilder sb = new StringBuilder("[");
    sb.append("op:" + getName() + " count: " + counter.intValue());
    sb.append("]");
    return sb.toString();
  }


  @Override
  public void measure(int count) {
    counter.addAndGet(count);
  }
}
