SELECT version() AS version, current_database() AS database;
SELECT content,dbid,role,preferred_role,mode,status,port FROM gp_segment_configuration ORDER BY content,dbid;
SELECT n.nspname,c.relname,pg_get_table_distributedby(c.oid) AS distribution,am.amname AS storage,c.reloptions FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_am am ON am.oid=c.relam WHERE n.nspname IN ('core','dm','staging','dq') AND c.relkind='r' ORDER BY 1,2;
SELECT name,setting,unit FROM pg_settings WHERE name IN ('optimizer','statement_mem','work_mem','gp_resource_manager','gp_vmem_protect_limit','TimeZone') ORDER BY name;
SELECT n.nspname,c.relname,p.policytype,p.numsegments,p.distkey::text AS distkey,p.distclass::text AS hash_opclasses FROM gp_distribution_policy p JOIN pg_class c ON c.oid=p.localoid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('core','dm','staging','dq') ORDER BY 1,2;
