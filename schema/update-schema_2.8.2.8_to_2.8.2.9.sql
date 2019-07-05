ALTER TABLE `hdfs_inodes` CHANGE `num_xattrs` `num_user_xattrs` tinyint(4) NOT NULL DEFAULT '0';
ALTER TABLE `hdfs_inodes` ADD COLUMN `num_sys_xattrs` tinyint(4) NOT NULL DEFAULT '0';
ALTER TABLE `hdfs_xattrs` MODIFY COLUMN `value` varbinary(13500) DEFAULT "";

CREATE TABLE `hdfs_encryption_zone` (
  `inode_id` bigint(20) NOT NULL,
  `zone_info` VARBINARY(13500),
  PRIMARY KEY (`inode_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1';

/* Cloud storage changes */
ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_quota_cloud` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_used_cloud` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_quota_update` ADD COLUMN `typespace_delta_cloud` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_block_infos` ADD COLUMN `cloud_bucket_id` SMALLINT(4) NOT NULL DEFAULT -1;

ALTER TABLE `hdfs_invalidated_blocks` ADD COLUMN `cloud_bucket_id` SMALLINT(4) NOT NULL DEFAULT -1;

CREATE TABLE `hdfs_provided_block_cached_location` (`block_id` bigint NOT NULL, `storage_id` int DEFAULT NULL, PRIMARY KEY (`block_id`));
