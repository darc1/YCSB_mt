
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

  public MTWorkload() {
    super();
  }

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    this.fieldnames.add(TENANT_ID_FIELD);
    this.tenantManager = TenantManager.getInstance();
    this.tenantManager.init(p);

    long insertstart = Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    long insertcount = Integer
        .parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertstart)));
    double missRatioPercent = Double.valueOf(p.getProperty(MISS_RATIO_PROPERTY, MISS_RATIO_DEFAULT));
    maxVal = (insertstart + insertcount - 1);
    long missingRecordsCount = (long)(maxVal * (missRatioPercent));
    adjustedMaxVal = (long) (maxVal + missingRecordsCount);
    System.out.format("adjusted max val to: %d from: %d miss ration is %f percent\n", adjustedMaxVal, maxVal,
        missRatioPercent);
    transactioninsertkeysequence = new AcknowledgedCounterGenerator(recordcount + missingRecordsCount);
    keychooser = new UniformLongGenerator(insertstart, adjustedMaxVal);
    double unauthRatioPercent = Double.valueOf(p.getProperty(UNAUTH_RATIO_PROPERTY, UNAUTH_RATIO_DEFAULT));
    unauthCount = Math.round(adjustedMaxVal * unauthRatioPercent);
    System.out.println("unauthorized records count: " + unauthCount + " unauthorized precent: " + unauthRatioPercent);

  }

  /**
   * Builds values for all fields.
   */
  protected HashMap<String, ByteIterator> buildValuesWithTenant(String key, int keynum) {
    HashMap<String, ByteIterator> values = super.buildValues(key);
    ByteIterator tenantIdValue = tenantManager.getTenantIdBytesForKey(key);
    measurements.measure(Measurements.MEASURE_KEY_TENANT_SPREAD, tenantManager.getTenantIndex(key));
    if (unauthCount > 0 && keynum > maxVal - unauthCount) {
      //System.out.println("Created record for invalid tenant.");
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
    //System.out.println("running transaction with keynum: " + keynum + " max val: " + maxVal);
    if (keynum >= maxVal) {
      //System.out.println("Got a miss query");
      measurements.measure(Measurements.MEASURE_READ_MISS, 1);
    }else if(keynum < maxVal && keynum > maxVal - unauthCount){
      measurements.measure(Measurements.MEASURE_READ_UNAUTH, 1);
    } else {
      measurements.measure(Measurements.MEASURE_READ_VALID, 1);
    }

    String keyname = getDbKey(keynum);

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
    db.read(table, keyname, fields, cells);

    if (dataintegrity) {
      verifyRow(keyname, cells);
    }
    return true;
  }
}
