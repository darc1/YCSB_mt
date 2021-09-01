
alter table usertable enable row level security;
alter table usertable2m enable row level security;
alter table usertable20m enable row level security;

-- Policy with 1000 rows
CREATE TABLE IF NOT EXISTS acls1k (
  pkey serial PRIMARY KEY,
  user_name VARCHAR(50),
  tenant_id VARCHAR(36),
  CONSTRAINT user_name_unique1k UNIQUE (user_name)
);

/*
--original function
create or replace function tenant_read_policy1k(varchar(36), text) 
returns bool 
as $$
begin
  return ((select tenant_id from acls1k where user_name = $2) = $1);
end;
$$
language plpgsql;

-- original policies
create policy tenant_read1k on usertable for select 
using(tenant_read_policy1k(tenant_id, current_user));
create policy tenant_read1k on usertable2m for select 
using(tenant_read_policy1k(tenant_id, current_user));
create policy tenant_read1k on usertable20m for select 
using(tenant_read_policy1k(tenant_id, current_user));
*/
-- drop policy tenant_read on usertable;
-- drop policy tenant_read on usertable2m;
-- drop policy tenant_read on usertable20m;


create or replace function tenant_read_policy1kv2(tid varchar(36), auser varchar(63)) 
returns bool 
as $$
begin
  return ((select tenant_id from acls1k where user_name = auser) = tid);
end;
$$
language plpgsql;
-- drop existing policies
do $$
declare xpolicy text;
declare xtable text;
begin
      for xpolicy,xtable in select policyname,tablename from pg_policies
        loop
          execute format('DROP POLICY %s ON %s'::text, xpolicy,  xtable) ;
        end loop;
end $$;

create policy tenant_read1kv2 on usertable for select 
using(tenant_read_policy1kv2(tenant_id, current_user::text));
create policy tenant_read1kv2 on usertable2m for select 
using(tenant_read_policy1kv2(tenant_id, current_user::text));
create policy tenant_read1kv2 on usertable20m for select 
using(tenant_read_policy1kv2(tenant_id, current_user::text));
/*
*/

-- Policy with 10000k rows
CREATE TABLE IF NOT EXISTS acls10k (
  pkey serial PRIMARY KEY,
  user_name VARCHAR(50),
  tenant_id VARCHAR(36),
  CONSTRAINT user_name_unique UNIQUE (user_name)
);

/*
-- original function and policy
create or replace function tenant_read_policy10k(varchar(36), text) 
returns bool 
as $$
begin
  return ((select tenant_id from acls10k where user_name = $2) = $1);
end;
$$
language plpgsql;

create policy tenant_read10k on usertable for select 
using(tenant_read_policy10k(tenant_id, current_user));
create policy tenant_read10k on usertable2m for select 
using(tenant_read_policy10k(tenant_id, current_user));
create policy tenant_read10k on usertable20m for select 
using(tenant_read_policy10k(tenant_id, current_user));
*/

create or replace function tenant_read_policy10kv2(varchar(36), text) 
returns bool 
as $$
begin
  return ((select tenant_id from acls10k where user_name = $2) = $1);
end;
$$
language plpgsql;

-- drop policy tenant_read on usertable;
-- drop policy tenant_read on usertable2m;
-- drop policy tenant_read on usertable20m;

-- drop existing policies
do $$
declare xpolicy text;
declare xtable text;
begin
      for xpolicy,xtable in select policyname,tablename from pg_policies
        loop
          execute format('DROP POLICY %s ON %s'::text, xpolicy,  xtable) ;
        end loop;
end $$;


create policy tenant_read10k on usertable for select 
using(tenant_read_policy10k(tenant_id, current_user));
create policy tenant_read10k on usertable2m for select 
using(tenant_read_policy10k(tenant_id, current_user));
create policy tenant_read10k on usertable20m for select 
using(tenant_read_policy10k(tenant_id, current_user));

-- Utility Drop all Policies
do $$
declare xpolicy text;
declare xtable text;
begin
      for xpolicy,xtable in select policyname,tablename from pg_policies
        loop
          execute format('DROP POLICY %s ON %s'::text, xpolicy,  xtable) ;
        end loop;
end $$;

-- Utility Drop all Roles
do $$
  declare rolename text;
  begin
    for rolename in select rolname from pg_roles where rolname like 'p10%'
      loop
        execute 'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM ' || rolename ;
        execute 'DROP ROLE ' || rolename;
      end loop;
  end $$;
