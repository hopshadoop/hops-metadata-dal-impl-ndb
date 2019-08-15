ALTER TABLE `hdfs_inodes` CHANGE `num_xattrs` `num_user_xattrs` tinyint(4) NOT NULL DEFAULT '0';
ALTER TABLE `hdfs_inodes` ADD COLUMN `num_sys_xattrs` tinyint(4) NOT NULL DEFAULT '0';
ALTER TABLE `hdfs_xattrs` MODIFY COLUMN `value` varbinary(13500) DEFAULT "";

CREATE TABLE `hdfs_encryption_zone` (
  `inode_id` bigint(20) NOT NULL,
  `zone_info` VARBINARY(13500),
  PRIMARY KEY (`inode_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1';
