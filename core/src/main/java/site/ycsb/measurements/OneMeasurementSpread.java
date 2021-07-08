
package site.ycsb.measurements;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import site.ycsb.measurements.exporter.*;

/**
 *Measurement Counter.
 * */
public class OneMeasurementSpread extends OneMeasurement {

  private final ConcurrentMap<Integer, AtomicInteger> counters;

  public OneMeasurementSpread(String name) {
    super(name);
    counters = new ConcurrentHashMap<Integer, AtomicInteger>();
  }

  private double getSum(){

    double sum = 0;
    for(AtomicInteger value : counters.values()){
      sum += value.doubleValue();
    }
    return sum;
  }

  @Override
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException {

    double sum = getSum(); 
    for(Entry<Integer, AtomicInteger> entry : counters.entrySet()){
      exporter.write(getName(), String.valueOf(entry.getKey()), entry.getValue().doubleValue()/sum);
    }
  }

  @Override
  public String getSummary() {
    double sum = getSum();
    StringBuilder sb = new StringBuilder("[");
    sb.append("op:" + getName() + " sum: " + sum);
    for(Entry<Integer, AtomicInteger> entry : counters.entrySet()){
      sb.append(String.valueOf(entry.getKey()) + ": "+ entry.getValue().doubleValue()/sum + "\n");
    }
    sb.append("]");
    return sb.toString();
  }


  @Override
  public void measure(int count) {
    AtomicInteger value  = counters.putIfAbsent(count, new AtomicInteger());
    if(value == null){
      value = counters.get(count);
    }
    value.incrementAndGet();
  }
}
