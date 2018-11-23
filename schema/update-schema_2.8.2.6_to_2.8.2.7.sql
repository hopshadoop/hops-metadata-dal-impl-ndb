insert into hdfs_variables (id, value) select 35, "" where (select count(*) from hdfs_variables)>0;
insert into hdfs_variables (id, value) select 36, "" where (select count(*) from hdfs_variables)>0;