ALTER TABLE `hdfs_inodes` CHANGE `num_xattrs` `num_user_xattrs` tinyint(4) NOT NULL DEFAULT '0';
ALTER TABLE `hdfs_inodes` ADD COLUMN `num_sys_xattrs` tinyint(4) NOT NULL DEFAULT '0';
ALTER TABLE `hdfs_xattrs` MODIFY COLUMN `value` varbinary(13500) DEFAULT "";

CREATE TABLE `hdfs_encryption_zone` (
  `inode_id` bigint(20) NOT NULL,
  `zone_info` VARBINARY(13500),
  PRIMARY KEY (`inode_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs COMMENT='NDB_TABLE=READ_BACKUP=1';

CREATE TABLE `yarn_conf_mutation` (
  `index` int(11) NOT NULL,
  `mutation` varbinary(13000) NOT NULL,
  PRIMARY KEY (`index`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

CREATE TABLE `yarn_conf` (
  `index` int(11) NOT NULL,
  `conf` blob NOT NULL,
  PRIMARY KEY (`index`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_used_provided` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_directory_with_quota_feature` ADD COLUMN `typespace_quota_provided` bigint(20) NOT NULL DEFAULT '-1';

ALTER TABLE `hdfs_quota_update` ADD COLUMN `typespace_delta_provided` bigint(20) NOT NULL DEFAULT '-1';

DROP TABLE `yarn_containerid_toclean`;

DROP TABLE `yarn_container_to_signal`;

DROP TABLE `yarn_container_to_decrease`;

DROP TABLE `yarn_containerstatus`;

DROP TABLE `yarn_rmnode_applications`;

DROP TABLE `yarn_nextheartbeat`;

DROP TABLE `yarn_pendingevents`;

DROP TABLE `yarn_resource`;

DROP TABLE `yarn_rms_load`;

DROP TABLE `yarn_rmnode`;

DROP TABLE `yarn_updatedcontainerinfo`;

DROP TABLE `yarn_containers_logs`;

DROP TABLE `yarn_containers_checkpoint`;

insert into hdfs_variables (id, value) select 37, "" where (select count(*) from hdfs_variables)>0;

insert into hdfs_variables (id, value) select 38, "" where (select count(*) from hdfs_variables)>0;
