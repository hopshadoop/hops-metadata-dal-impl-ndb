-- create an index on hdfs_retry_cache_entry.epoch
-- check if the create index fix is not already applied 

SELECT COUNT(*) INTO @index_exists FROM information_schema.statistics WHERE table_schema = 'hops' AND table_name = 'hdfs_retry_cache_entry' AND index_name = 'epoch_idx';

SET @create_index_stmt = IF(@index_exists = 0, 'ALTER TABLE hdfs_retry_cache_entry ADD INDEX epoch_idx (epoch);', 'SELECT "Index already exists";');
PREPARE stmt FROM @create_index_stmt;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
