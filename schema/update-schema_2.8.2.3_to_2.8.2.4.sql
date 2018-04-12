CREATE TABLE `hdfs_storages` (
  `host_id` varchar(255) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `storage_type` int(11) NOT NULL,
  PRIMARY KEY (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

ALTER TABLE `hdfs_inodes` ADD COLUMN `storage_policy` bit(8) NOT NULL DEFAULT 0;

ALTER TABLE `hdfs_replica_under_constructions` ADD COLUMN `chosen_as_primary`  tinyint NOT NULL DEFAULT 0;

ALTER TABLE `hdfs_inodes` ADD COLUMN `children_num` int(11) NOT NULL DEFAULT 0;

CREATE TABLE `hdfs_retry_cache_entry` (
  `client_id` VARBINARY(16) NOT NULL,
  `call_id` INT NOT NULL,
  `payload` VARBINARY(13500) NULL,
  `expiration_time` BIGINT(20) NULL,
  `state` BIT(8) NULL,
  PRIMARY KEY (`client_id`, `call_id`),
  INDEX `expiration_time` (`expiration_time` ASC)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
 
ALTER TABLE `hdfs_pending_blocks`
ADD COLUMN `target` varchar(255),
DROP COLUMN `num_replicas_in_progress`,
DROP PRIMARY KEY,
ADD PRIMARY KEY (`inode_id`,`block_id`, `target`);
