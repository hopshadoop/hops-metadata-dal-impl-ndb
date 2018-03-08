CREATE TABLE `hdfs_storages` (
  `host_id` varchar(255) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `storage_type` int(11) NOT NULL,
  PRIMARY KEY (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

ALTER TABLE `hdfs_inodes` ADD COLUMN `storage_policy` bit(8) NOT NULL DEFAULT 0;

ALTER TABLE `hdfs_replica_under_constructions` ADD COLUMN `chosen_as_primary`  tinyint NOT NULL DEFAULT 0;
