
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
	serial PRIMARY KEY,
	user_name VAR_CHAR(50),
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

load the database:
```
./bin/ycsb.sh load jdbc -P mt.properties
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
