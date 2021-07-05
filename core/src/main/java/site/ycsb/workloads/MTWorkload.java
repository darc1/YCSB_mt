
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
import site.ycsb.StringByteIterator;
import site.ycsb.WorkloadException;
import site.ycsb.generator.UniformLongGenerator;

import java.util.*;

/**
 * Workload for multi-tenancy.
 */
public class MTWorkload extends CoreWorkload {
  public static final String TENANT_ID_FIELD = "tenant_id";
  public static final String NUM_TENANTS_DEFAULT = "10";
  public static final String NUM_TENANTS_PROPERTY = "num_tenants";
  public static final String MISS_RATIO_PERCENT_DEFAULT = "1";
  public static final String MISS_RATIO_PERCENT_PROPERTY = "num_tenants";
  private int numTenants;
  private List<ByteIterator> tenantIds;

  public MTWorkload() {
    super();
  }

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);
    this.fieldnames.add(TENANT_ID_FIELD);
    numTenants = Integer.parseInt(p.getProperty(NUM_TENANTS_PROPERTY, NUM_TENANTS_DEFAULT));
    tenantIds = new ArrayList<>(numTenants);
    for (int i = 0; i < numTenants; i++) {
      String uuid = UUID.randomUUID().toString();
      tenantIds.add(new StringByteIterator(uuid));
    }
    long insertstart = Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    long insertcount = Integer
        .parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertstart)));
    int missRatioPercent = Integer.valueOf(p.getProperty(MISS_RATIO_PERCENT_PROPERTY, MISS_RATIO_PERCENT_DEFAULT));
    long maxVal = (insertstart + insertcount - 1);
    long adjustedMaxVal = (long) (maxVal + maxVal * (missRatioPercent / 100.0));
    System.out.format("adjusted max val to: %d from: %d miss ration is %d percent\n", adjustedMaxVal, maxVal,
        missRatioPercent);
    keychooser = new UniformLongGenerator(insertstart, adjustedMaxVal);

    // TODO create users!!! for each tenant!!
  }

  private void createUsers(){
  }

  /**
   * Builds values for all fields.
   */
  @Override
  protected HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = super.buildValues(key);
    values.put(TENANT_ID_FIELD, getTenantIdForKey(key));
    return values;
  }

  @Override
  protected String getDbKey(long keynum) {
    //return CoreWorkload.getKeyNumValue(keynum, orderedinserts);
    return null;
  }

  private ByteIterator getTenantIdForKey(String key){
    int index = Math.abs(hashCode()) % tenantIds.size();
    return tenantIds.get(index);
  }
}
