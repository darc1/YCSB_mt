
Create Schemas for Multi-tenancy

```sql
CREATE TABLE usertable (
	YCSB_KEY VARCHAR(255) PRIMARY KEY,
	FIELD0 TEXT, FIELD1 TEXT,
	FIELD2 TEXT, FIELD3 TEXT,
	FIELD4 TEXT, FIELD5 TEXT,
	FIELD6 TEXT, FIELD7 TEXT,
	FIELD8 TEXT, FIELD9 TEXT,
	tenant_id VARCHAR(36)
);

CREATE TABLE acls (
	pkey serial PRIMARY KEY,
	user_name VARCHAR(50),
	tenant_id VARCHAR(36),
	CONSTRAINT user_name_unique UNIQUE (user_name)
);

create or replace function tenant_read_policy(varchar(36), text) 
returns bool 
as $$
begin
              return ((select tenant_id from acls where user_name = $2) = $1);

end;
$$
language plpgsql;

create policy tenant_read on usertable for select 
using(tenant_read_policy(tenant_id, current_user));

alter table usertable enable row level security;
```

to build:
```
mvn clean package -P ycsb-release -DskipTests
```

to run:

create tenants and users:
```
./workdir/bin/ycsb.sh tenants jdbcmt -P mt.properties
```
properties:
```
num_tenants = number of tenants - default: 10
num_users_per_tenant = number of users per tenant - default: 1
```

load the database:
```
./workdir/bin/ycsb.sh load jdbcmt -P mt.properties
```
properties:
```
miss_ratio = ratio of miss queries - default: 0.01
unauth_ratio = ratio of unauthorized access - default: 0.01
```

run the benchmark:
```
./workdir/bin/ycsb.sh run jdbcmt -P mt.properties
```

properties file:
```
recordcount=1000
operationcount=1000
workload=site.ycsb.workloads.MTWorkload
db.url=jdbc:postgresql://localhost:5432/bench
db.user=postgres
db.passwd=docker

readallfields=true

readproportion=1
updateproportion=0
scanproportion=0
insertproportion=0

requestdistribution=zipfian
```
