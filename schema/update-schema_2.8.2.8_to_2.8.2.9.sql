ALTER TABLE `hdfs_inodes` CHANGE `num_xattrs` `num_user_xattrs` tinyint(4) NOT NULL DEFAULT '0';
ALTER TABLE `hdfs_inodes` ADD COLUMN `num_sys_xattrs` tinyint(4) NOT NULL DEFAULT '0';