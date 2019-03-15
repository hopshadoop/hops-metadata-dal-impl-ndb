ALTER TABLE `hdfs_le_descriptors` ADD COLUMN `location_domain_id` tinyint(4) default 0;

ALTER TABLE `yarn_le_descriptors` ADD COLUMN `location_domain_id` tinyint(4) default 0;

ALTER TABLE `hdfs_hash_buckets` DROP COLUMN `hash`;

ALTER TABLE `hdfs_hash_buckets` ADD COLUMN `hash` binary(20) NOT NULL DEFAULT '0';

ALTER TABLE hdfs_replicas ADD KEY `storage_and_bucket_idx` (`storage_id`, `bucket_id`);

ALTER TABLE hdfs_inodes DROP KEY `inode_idx`;

ALTER TABLE hdfs_inodes ADD UNIQUE KEY `inode_idx` (`id`);

ALTER TABLE `hdfs_block_infos` ADD COLUMN `truncate_block_num_bytes` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_block_infos` ADD COLUMN `truncate_block_generation_stamp` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_inode_attributes` RENAME TO `hdfs_directory_with_quota_feature`;

ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_quota_disk` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_quota_ssd` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_quota_raid5` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_quota_archive` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_used_disk` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_used_ssd` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_used_raid5` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_used_archive` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_quota_update` ADD COLUMN `typespace_delta_disk` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_quota_update` ADD COLUMN `typespace_delta_ssd` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_quota_update` ADD COLUMN `typespace_delta_raid5` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_quota_update` ADD COLUMN `typespace_delta_archive` bigint(20) NOT NULL DEFAULT '-1';
