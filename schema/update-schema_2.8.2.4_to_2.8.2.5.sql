ALTER TABLE `hdfs_replica_under_constructions` DROP COLUMN `bucket_id`;
ALTER TABLE `hdfs_replicas` DROP INDEX `hash_bucket_idx`;
ALTER TABLE `hdfs_replicas` DROP COLUMN `bucket_id`;
