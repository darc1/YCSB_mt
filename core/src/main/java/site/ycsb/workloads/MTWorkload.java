
/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016-2020 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.workloads;

import site.ycsb.Status;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.WorkloadException;
import site.ycsb.generator.UniformLongGenerator;
import site.ycsb.generator.*;
import site.ycsb.measurements.Measurements;
import site.ycsb.mt.TenantManager;

import java.util.*;

/**
 * Workload for multi-tenancy.
 */
public class MTWorkload extends CoreWorkload {
  public static final String TENANT_ID_FIELD = "tenant_id";
  public static final String MISS_RATIO_DEFAULT = "0.01";
  public static final String MISS_RATIO_PROPERTY = "miss_ratio";
  public static final String UNAUTH_RATIO_DEFAULT = "0.01";
  public static final String UNAUTH_RATIO_PROPERTY = "unauth_ratio";
  private TenantManager tenantManager;
  private long maxVal;
  private long adjustedMaxVal;
  private long unauthCount;
  private boolean logKeys;

  public MTWorkload() {
    super();
  }

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    this.fieldnames.add(TENANT_ID_FIELD);
    this.tenantManager = TenantManager.getInstance();
    this.tenantManager.init(p);

    logKeys = Boolean.parseBoolean(p.getProperty("log_keys", "false"));
    long insertstart = Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    long insertcount = Integer
        .parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertstart)));
    double missRatio = Double.valueOf(p.getProperty(MISS_RATIO_PROPERTY, MISS_RATIO_DEFAULT));
    if(missRatio >= 1){
      System.out.println("miss_ration >= 1 setting to: 0.99");
      missRatio = 0.99;
    }
    maxVal = (insertstart + insertcount - 1);
    adjustedMaxVal = (long) (maxVal/(1 - missRatio));
    long missingRecordsCount = adjustedMaxVal - maxVal;
    System.out.format("adjusted max val to: %d from: %d miss ration is %f percent\n", adjustedMaxVal, maxVal,
        missRatio);
    transactioninsertkeysequence = new AcknowledgedCounterGenerator(recordcount + missingRecordsCount);
    keychooser = new UniformLongGenerator(insertstart, adjustedMaxVal);
    double unauthRatio = Double.valueOf(p.getProperty(UNAUTH_RATIO_PROPERTY, UNAUTH_RATIO_DEFAULT));
    if(unauthRatio >= 0.1){
      System.out.println("unauth_ratio >= 0.1 setting to: 0.1");
      unauthRatio = 0.1;
    }
    unauthCount = Math.round(adjustedMaxVal * (1 - unauthRatio)) - adjustedMaxVal;
    System.out.println("unauthorized records count: " + unauthCount + " unauthorized precent: " + unauthRatio);

  }

  /**
   * Builds values for all fields.
   */
  protected HashMap<String, ByteIterator> buildValuesWithTenant(String key, int keynum) {
    HashMap<String, ByteIterator> values = super.buildValues(key);
    ByteIterator tenantIdValue = tenantManager.getTenantIdBytesForKey(key);
    measurements.measure(Measurements.MEASURE_KEY_TENANT_SPREAD, tenantManager.getTenantIndex(key));
    if (unauthCount > 0 && keynum > maxVal - unauthCount) {
      // System.out.println("Created record for invalid tenant.");
      tenantIdValue = tenantManager.getInvalidTenant();
    }
    values.put(TENANT_ID_FIELD, tenantIdValue);
    return values;
  }

  @Override
  protected String getDbKey(long keynum) {
    return getKeyNumValue(keynum);
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int keynum = keysequence.nextValue().intValue();
    String dbkey = getDbKey(keynum);
    HashMap<String, ByteIterator> values = buildValuesWithTenant(dbkey, keynum);

    return super.doInsertInternal(db, dbkey, values);
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {

    long keynum = nextKeynum();
    String measureName = null;
    // System.out.println("running transaction with keynum: " + keynum + " max val:
    // " + maxVal);
    if (keynum >= maxVal) {
      // System.out.println("Got a miss query");
      measurements.measure(Measurements.MEASURE_READ_MISS, 1);
      measureName = "X-" + Measurements.MEASURE_READ_MISS;
      // System.out.println("miss key val: " + getDbKey(keynum));
    } else if (keynum < maxVal && keynum > maxVal - unauthCount) {
      measurements.measure(Measurements.MEASURE_READ_UNAUTH, 1);
      measureName = "X-" + Measurements.MEASURE_READ_UNAUTH;
    } else {
      measurements.measure(Measurements.MEASURE_READ_VALID, 1);
      measureName = "X-" + Measurements.MEASURE_READ_VALID;
    }

    String keyname = getDbKey(keynum);
    if (logKeys) {
      MTThreadState state = (MTThreadState)threadstate;
      System.out.println("keys: " + keyname + " " + keynum + " thread id: " + state.getThreadid() + " start.");
    }
    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    } else if (dataintegrity) {
      // pass the full field list if dataintegrity is on for verification
      fields = new HashSet<String>(fieldnames);
    }

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    Status status = db.read(table, keyname, fields, cells);
    measurements.measure(measureName, status.getElapsed());
    if (dataintegrity) {
      verifyRow(keyname, cells);
    }
    if (logKeys) {
      MTThreadState state = (MTThreadState)threadstate;
      System.out.println("keys: " + keyname + " " + keynum + " thread id: " + state.getThreadid() + " complete.");
    }
    return true;
  }

  @Override
  public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
    return new MTThreadState(mythreadid);
  }

  class MTThreadState {
    private int threadid;

    MTThreadState(int threadid) {
      this.threadid = threadid;
    }

    public int getThreadid() {
      return threadid;
    }
  }

}
