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