
package site.ycsb.mt;

import java.util.*;
import site.ycsb.StringByteIterator;
import site.ycsb.ByteIterator;

/**
 * Tenant Manager Class.
 */
public final class TenantManager {

  private boolean isInitialized;
  private static TenantManager instance;
  private int seed = 90901;
  private int numTenants;
  private int usersPerTenant;
  private List<ByteIterator> tenantIdsBytes;
  private List<String> tenantIds;
  private Map<String, List<String>> tenantUsers;
  private String invalidTenantId;
  private ByteIterator invalidTenantIdBytes;

  // Properties
  public static final String NUM_TENANTS_DEFAULT = "10";
  public static final String NUM_TENANTS_PROPERTY = "num_tenants";
  public static final String NUM_USERS_PER_TENANT_DEFAULT = "1";
  public static final String NUM_USERS_PER_TENANT_PROPERTY = "num_users_per_tenant";
  public static final String MULTI_TENANT_INIT = "multi_teant_init";

  public static TenantManager getInstance() {
    if (instance == null) {
      instance = new TenantManager();
    }
    return instance;
  }

  private TenantManager() {

  }

  public synchronized void init(Properties p) {
    if (isInitialized) {
      return;
    }

    initTenants(p);

    isInitialized = true;
  }

  public void initTenants(Properties p) {

    numTenants = Integer.parseInt(p.getProperty(NUM_TENANTS_PROPERTY, NUM_TENANTS_DEFAULT));
    usersPerTenant = Integer.parseInt(p.getProperty(NUM_USERS_PER_TENANT_PROPERTY, NUM_USERS_PER_TENANT_DEFAULT));
    tenantUsers = new HashMap<String, List<String>>();
    tenantIds = new ArrayList<String>();
    tenantIdsBytes = new ArrayList<ByteIterator>();
    Random tenantRandom = new Random(seed);
    for (int i = 0; i < numTenants; i++) {
      String uuid = generateUuid(tenantRandom);
      System.out.println("Created tenant ID: " + uuid);
      tenantIds.add(uuid);
      tenantIdsBytes.add(new StringByteIterator(uuid));
      tenantUsers.put(uuid, new ArrayList<String>());
      for (int user = 0; user < usersPerTenant; user++) {
        String username = "user" + user + "t" + i;
        tenantUsers.get(uuid).add(username);
      }
    }

    invalidTenantId = generateUuid(tenantRandom);
    System.out.println("Created invalid tenant ID: " + invalidTenantId);
    invalidTenantIdBytes = new StringByteIterator(invalidTenantId);

  }

  private String generateUuid(Random tenantRandom) {
    byte[] name = new byte[8];
    tenantRandom.nextBytes(name);
    String uuid = UUID.nameUUIDFromBytes(name).toString();
    return uuid;
  }

  public int getTenantIndex(String key){
    int index = Math.abs(key.hashCode()) % tenantIds.size();
    return index;
  }

  public ByteIterator getTenantIdBytesForKey(String key) {
    int index = getTenantIndex(key);
    return tenantIdsBytes.get(index);
  }

  public String getTenantIdForKey(String key) {
    int index = getTenantIndex(key);
    return tenantIds.get(index);
  }
  public int getNumTenants() {
    return numTenants;
  }

  public Set<String> getTenantIds() {
    return tenantUsers.keySet();
  }

  public List<String> getTenantUsers(String tenantId) {
    return tenantUsers.get(tenantId);
  }

  public ByteIterator getInvalidTenant() {
    return invalidTenantIdBytes;
  }

}
