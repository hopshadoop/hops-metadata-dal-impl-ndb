delimiter $$

CREATE TABLE `hdfs_block_infos` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `block_index` int(11) DEFAULT NULL,
  `num_bytes` bigint(20) DEFAULT NULL,
  `generation_stamp` bigint(20) DEFAULT NULL,
  `block_under_construction_state` int(11) DEFAULT NULL,
  `time_stamp` bigint(20) DEFAULT NULL,
  `primary_node_index` int(11) DEFAULT NULL,
  `block_recovery_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inode_id`,`block_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_block_lookup_table` (
  `block_id` bigint(20) NOT NULL,
  `inode_id` int(11) NOT NULL,
  PRIMARY KEY (`block_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `hdfs_corrupt_replicas` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `timestamp` bigint(20) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`),
  KEY `timestamp` (`timestamp`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_excess_replicas` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`),
  KEY `storage_idx` (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_inode_attributes` (
  `inodeId` int(11) NOT NULL,
  `nsquota` bigint(20) DEFAULT NULL,
  `dsquota` bigint(20) DEFAULT NULL,
  `nscount` bigint(20) DEFAULT NULL,
  `diskspace` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inodeId`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `hdfs_inodes` (
  `partition_id` int(11) NOT NULL,
  `parent_id` int(11) NOT NULL DEFAULT '0',
  `name` varchar(255) NOT NULL DEFAULT '',
  `id` int(11) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `group_id` int(11) DEFAULT NULL,
  `modification_time` bigint(20) DEFAULT NULL,
  `access_time` bigint(20) DEFAULT NULL,
  `permission` smallint(6) DEFAULT NULL,
  `client_name` varchar(100) DEFAULT NULL,
  `client_machine` varchar(100) DEFAULT NULL,
  `client_node` varchar(100) DEFAULT NULL,
  `generation_stamp` int(11) DEFAULT NULL,
  `header` bigint(20) DEFAULT NULL,
  `symlink` varchar(255) DEFAULT NULL,
  `subtree_lock_owner` bigint(20) DEFAULT NULL,
  `size` bigint(20) NOT NULL DEFAULT '0',
  `quota_enabled` bit(8) NOT NULL,
  `meta_enabled` bit(8) DEFAULT b'110000',
  `is_dir` bit(8) NOT NULL,
  `under_construction` bit(8) NOT NULL,
  `subtree_locked` bit(8) DEFAULT NULL,
  PRIMARY KEY (`partition_id`,`parent_id`,`name`),
  KEY `pidex` (`parent_id`),
  KEY `inode_idx` (`id`),
  KEY `c1` (`parent_id`,`partition_id`),
  KEY `c2` (`partition_id`,`parent_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (partition_id) */  $$

delimiter $$

CREATE TABLE `hdfs_users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY (`name`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$

delimiter $$

CREATE TABLE `hdfs_groups` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY (`name`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$

delimiter $$

CREATE TABLE `hdfs_users_groups` (
  `user_id` int(11) NOT NULL,
  `group_id` int(11)  NOT NULL,
  PRIMARY KEY (`user_id`, `group_id`),
  CONSTRAINT `user_id`
    FOREIGN KEY (`user_id`)
    REFERENCES `hdfs_users` (`id`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION,
 CONSTRAINT `group_id`
    FOREIGN KEY (`group_id`)
    REFERENCES `hdfs_groups` (`id`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$

delimiter $$

CREATE TABLE `hdfs_invalidated_blocks` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `generation_stamp` bigint(20) DEFAULT NULL,
  `num_bytes` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`),
  KEY `storage_idx` (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_le_descriptors` (
  `id` bigint(20) NOT NULL,
  `counter` bigint(20) NOT NULL,
  `hostname` varchar(25) NOT NULL,
  `httpAddress` varchar(100) DEFAULT NULL,
  `partition_val` int(11) NOT NULL DEFAULT '0',
PRIMARY KEY (`id`,`partition_val`),
KEY `part` (`partition_val`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (partition_val) */$$

delimiter $$

CREATE TABLE `yarn_le_descriptors` (
  `id` bigint(20) NOT NULL,
  `counter` bigint(20) NOT NULL,
  `hostname` varchar(25) NOT NULL,
  `httpAddress` varchar(100) DEFAULT NULL,
  `partition_val` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`,`partition_val`),
  KEY `part` (`partition_val`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (partition_val) */$$

delimiter $$

CREATE TABLE `hdfs_lease_paths` (
  `holder_id` int(11) NOT NULL,
  `path` varchar(3000) NOT NULL,
  PRIMARY KEY (`holder_id`,`path`),
  KEY `path_idx` (`path`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (holder_id) */$$

delimiter $$

CREATE TABLE `hdfs_leases` (
  `holder_id` int(11) NOT NULL DEFAULT '0',
  `holder` varchar(255) NOT NULL,
  `last_update` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`holder_id`,`holder`),
  KEY `update_idx` (`last_update`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (holder_id) */$$

delimiter $$

CREATE TABLE `hdfs_misreplicated_range_queue` (
  `range` varchar(120) NOT NULL,
  PRIMARY KEY (`range`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `hdfs_path_memcached` (
  `path` varchar(128) NOT NULL,
  `inodeids` varbinary(13500) NOT NULL,
  PRIMARY KEY (`path`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `hdfs_pending_blocks` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `time_stamp` bigint(20) NOT NULL,
  `num_replicas_in_progress` int(11) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_replica_under_constructions` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  `state` int(11) DEFAULT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_replicas` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `storage_id` int(11) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`,`storage_id`),
  KEY `storage_idx` (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_safe_blocks` (
  `id` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `hdfs_storage_id_map` (
  `storage_id` varchar(128) NOT NULL,
  `sid` int(11) NOT NULL,
  PRIMARY KEY (`storage_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `hdfs_under_replicated_blocks` (
  `inode_id` int(11) NOT NULL,
  `block_id` bigint(20) NOT NULL,
  `level` int(11) DEFAULT NULL,
  `timestamp` bigint(20) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_id`),
  KEY `level` (`level`,`timestamp`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_variables` (
  `id` int(11) NOT NULL,
  `value` varbinary(500) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `hdfs_quota_update` (
  `id` int(11) NOT NULL,
  `inode_id` int(11) NOT NULL,
  `namespace_delta` bigint(20) DEFAULT NULL,
  `diskspace_delta` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`inode_id`,`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$


delimiter $$

CREATE TABLE `hdfs_encoding_status` (
  `inode_id` int(11) NOT NULL,
  `status` int(11) DEFAULT NULL,
  `codec` varchar(8) DEFAULT NULL,
  `target_replication` smallint(11) DEFAULT NULL,
  `parity_status` int(11) DEFAULT NULL,
  `status_modification_time` bigint(20) DEFAULT NULL,
  `parity_status_modification_time` bigint(20) DEFAULT NULL,
  `parity_inode_id` int(11) DEFAULT NULL,
  `parity_file_name` char(36) DEFAULT NULL,
  `lost_blocks` int(11) DEFAULT 0,
  `lost_parity_blocks` int(11) DEFAULT 0,
  `revoked` bit(8) DEFAULT 0,
  PRIMARY KEY (`inode_id`),
  UNIQUE KEY `parity_inode_id` (`parity_inode_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$

delimiter $$

CREATE TABLE `hdfs_block_checksum` (
  `inode_id` int(11) NOT NULL,
  `block_index` int(11) NOT NULL,
  `checksum` bigint(20) NOT NULL,
  PRIMARY KEY (`inode_id`,`block_index`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1
/*!50100 PARTITION BY KEY (inode_id) */$$

delimiter $$

CREATE TABLE `hdfs_on_going_sub_tree_ops` ( 
  `partition_id` int(11) NOT NULL DEFAULT '0',                             
  `path` varchar(3000) NOT NULL,                                           
  `namenode_id` bigint(20) NOT NULL,                                       
  `op_name` int(11) NOT NULL,                                              
  PRIMARY KEY (`partition_id`,`path`),                                     
  KEY `partindex` (`partition_id`),                                        
  KEY `nameidx` (`path`)                                                   
) ENGINE=ndbcluster DEFAULT CHARSET=latin1                                 
/*!50100 PARTITION BY KEY (partition_id) */$$                              

delimiter $$

CREATE TABLE `hdfs_encoding_jobs` (
  `jt_identifier` varchar(50) NOT NULL,
  `job_id` int(11) NOT NULL,
  `path` varchar(3000) NOT NULL,
  `job_dir` varchar(200) NOT NULL,
  PRIMARY KEY (`jt_identifier`,`job_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$

delimiter $$

CREATE TABLE `hdfs_repair_jobs` (
  `jt_identifier` varchar(50) NOT NULL,
  `job_id` int(11) NOT NULL,
  `path` varchar(3000) NOT NULL,
  `in_dir` varchar(3000) NOT NULL,
  `out_dir` varchar(3000) NOT NULL,
  PRIMARY KEY (`jt_identifier`,`job_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$

delimiter $$

CREATE TABLE `yarn_applicationstate` (
  `applicationid` VARCHAR(45) NOT NULL,
  `appstate` VARBINARY(13500) NULL,
  `appuser` VARCHAR(45) NULL,
  `appname` VARCHAR(200) NULL,
  `appsmstate` VARCHAR(45) NULL,
PRIMARY KEY (`applicationid`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(`applicationid`)$$


delimiter $$

CREATE TABLE `yarn_delegation_token` (
  `seq_number` INT NOT NULL,
  `rmdt_identifier` VARBINARY(13500) NULL,
PRIMARY KEY (`seq_number`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `yarn_version` (
  `id` INT NOT NULL,
  `version` VARBINARY(13500) NULL,
PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `yarn_delegation_key` (
  `key` INT NOT NULL,
  `delegationkey` VARBINARY(13500) NULL,
PRIMARY KEY (`key`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `yarn_sequence_number` (
  `id` INT NOT NULL,
  `sequence_number` INT NULL,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `yarn_applicationattemptstate` (
  `applicationid` VARCHAR(45) NOT NULL,
  `applicationattemptid` VARCHAR(45) NOT NULL,
  `applicationattemptstate` VARBINARY(13000) NULL,
  `applicationattempthost` VARCHAR(255) NULL,
  `applicationattemptrpcport` INT NULL,
  `applicationattempttokens` VARBINARY(500) NULL,
  `applicationattempttrakingurl` VARCHAR(120) NULL,
  PRIMARY KEY (`applicationid`, `applicationattemptid`),
  INDEX `applicationid` (`applicationid` ASC),
  CONSTRAINT `applicationid`
  FOREIGN KEY (`applicationid`)
  REFERENCES `yarn_applicationstate` (`applicationid`)
  ON DELETE CASCADE
  ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(`applicationid`)$$


delimiter $$

CREATE TABLE `yarn_appmaster_rpc` (
  `rpcid` INT NOT NULL,
  `type` VARCHAR(45) NOT NULL,
  `rpc` VARBINARY(13000) NOT NULL,
  `userid` VARCHAR(250) NULL,
  PRIMARY KEY (`rpcid`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `yarn_variables` (
  `id` INT NOT NULL,
  `value` INT NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `yarn_queuemetrics` (
  `queue_name` VARCHAR(45) NOT NULL,
  `apps_submitted` INT NULL,
  `apps_running` INT NULL,
  `apps_pending` INT NULL,
  `apps_completed` INT NULL,
  `apps_killed` INT NULL,
  `apps_failed` INT NULL,
  `allocated_mb` INT NULL,
  `allocated_vcores` INT NULL,
  `allocated_containers` INT NULL,
  `aggregate_containers_allocated` BIGINT NULL,
  `aggregate_containers_released` BIGINT NULL,
  `available_mb` INT NULL,
  `available_vcores` INT NULL,
  `pending_mb` INT NULL,
  `pending_vcores` INT NULL,
  `reserved_mb` INT NULL,
  `reserved_vcores` INT NULL,
  `reserved_containers` INT NULL,
  `active_users` INT NULL,
  `active_applications` INT NULL,
  `parent_id` INT NULL,
  `pending_containers` INT NULL,
  PRIMARY KEY (`queue_name`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `yarn_rmnode` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `hostname` VARCHAR(255) NULL,
  `commandport` INT NULL,
  `httpport` INT NULL,
  `nodeaddress` VARCHAR(255) NULL,
  `httpaddress` VARCHAR(255) NULL,
  `nodeid` VARCHAR(255) NULL,
  `healthreport` VARCHAR(500) NULL,
  `lasthealthreporttime` BIGINT NULL,
  `currentstate` VARCHAR(45) NULL,
  `overcommittimeout` INT NULL,
  `nodemanager_version` VARCHAR(45) NULL,
  `pendingeventid` INT,
  PRIMARY KEY (`rmnodeid`))
ENGINE = ndbcluster DEFAULT CHARSET=latin1
PACK_KEYS = DEFAULT PARTITION BY KEY(rmnodeid)$$


delimiter $$

CREATE TABLE `yarn_resource` (
  `id` VARCHAR(255) NOT NULL,
  `type` INT NOT NULL,
  `parent` INT NOT NULL,
  `memory` INT NULL,
  `virtualcores` INT NULL,
  `pendingeventid` INT,
  PRIMARY KEY (`id`, `type`, `parent`),
  INDEX `id` (`id` ASC)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(id)$$


delimiter $$

CREATE TABLE `yarn_node` (
  `nodeid` VARCHAR(255) NOT NULL,
  `name` VARCHAR(255) NULL,
  `location` VARCHAR(255) NULL,
  `level` INT NULL,
  `parent` VARCHAR(255) NULL,
  `pendingeventid` INT,
  PRIMARY KEY (`nodeid`),
  INDEX `name` (`name` ASC, `location` ASC),
  CONSTRAINT `nodeid`
  FOREIGN KEY (`nodeid`)
  REFERENCES `yarn_rmnode` (`rmnodeid`)
  ON DELETE CASCADE
  ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(nodeid) $$


delimiter $$

CREATE TABLE `yarn_ficascheduler_node` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `nodename` VARCHAR(255) NULL,
  `numcontainers` INT NULL,
  `rmcontainerid` VARCHAR(45) NULL,
  PRIMARY KEY (`rmnodeid`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(`rmnodeid`)$$


delimiter $$

CREATE TABLE `yarn_rmctx_activenodes` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`rmnodeid`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `yarn_updatedcontainerinfo` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `containerid` VARCHAR(45) NOT NULL,
  `updatedcontainerinfoid` INT NOT NULL,
  `pendingeventid` INT,
  PRIMARY KEY (`rmnodeid`, `containerid`, `updatedcontainerinfoid`),
  INDEX `containerid` (`containerid` ASC),
  CONSTRAINT `rmnodeid`
  FOREIGN KEY (`rmnodeid`)
  REFERENCES `yarn_rmnode` (`rmnodeid`)
  ON DELETE CASCADE
  ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(`rmnodeid`)$$


delimiter $$

CREATE TABLE `yarn_containerstatus` (
  `containerid` VARCHAR(45) NOT NULL,
  `rmnodeid` VARCHAR(255) NOT NULL,
  `type` VARCHAR(45) NOT NULL,
  `state` VARCHAR(45) NULL,
  `diagnostics` VARCHAR(2000) NULL,
  `exitstatus` INT NULL,
  `pendingeventid` INT,
  PRIMARY KEY (`containerid`, `rmnodeid`, `type`),
  INDEX `rmnodeid_idx` (`rmnodeid` ASC),
  CONSTRAINT `rmnodeid`
  FOREIGN KEY (`rmnodeid`)
  REFERENCES `yarn_rmnode` (`rmnodeid`)
  ON DELETE CASCADE
  ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(`rmnodeid`)$$




delimiter $$



CREATE TABLE `yarn_justlaunchedcontainers` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `containerid` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`containerid`, `rmnodeid`),
  INDEX `rmnodeid_idx` (`rmnodeid` ASC),
  CONSTRAINT `rmnodeid`
    FOREIGN KEY (`rmnodeid`)
    REFERENCES `yarn_rmnode` (`rmnodeid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(`rmnodeid`)$$

delimiter $$

CREATE TABLE `yarn_latestnodehbresponse` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `response` VARBINARY(13500) NULL,
  PRIMARY KEY (`rmnodeid`),
  CONSTRAINT `rmnodeid`
    FOREIGN KEY (`rmnodeid`)
    REFERENCES `yarn_rmnode` (`rmnodeid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(rmnodeid) $$

delimiter $$

CREATE TABLE `yarn_updated_node` (
  `applicationid` VARCHAR(45) NOT NULL,
  `nodeid` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`applicationid`, `nodeid`),
  CONSTRAINT `nodeid`
    FOREIGN KEY (`nodeid`)
    REFERENCES `yarn_rmnode` (`rmnodeid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(applicationid) $$

delimiter $$

CREATE TABLE `yarn_ran_node` (
  `application_attempt_id` VARCHAR(45) NOT NULL,
  `nodeid` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`application_attempt_id`, `nodeid`),
  CONSTRAINT `nodeid`
    FOREIGN KEY (`nodeid`)
    REFERENCES `yarn_rmnode` (`rmnodeid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(application_attempt_id) $$

delimiter $$

CREATE TABLE `yarn_containerid_toclean` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `containerid` VARCHAR(45) NOT NULL,
  `pendingeventid` INT,
  PRIMARY KEY (`rmnodeid`, `containerid`),
  INDEX `rmnodeId` (`containerid` ASC),
  CONSTRAINT `rmnodeid`
    FOREIGN KEY (`rmnodeid`)
    REFERENCES `yarn_rmnode` (`rmnodeid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(rmnodeid) $$


delimiter $$



CREATE TABLE `yarn_rmcontainer` (
  `containerid_id` VARCHAR(45) NOT NULL,
  `appattemptid_id` VARCHAR(45) NOT NULL,
  `nodeid_id` VARCHAR(255) NULL,
  `user` VARCHAR(45) NULL,
  `starttime` BIGINT NULL,
  `finishtime` BIGINT NULL,
  `state` VARCHAR(45) NULL,
  `finishedstatusstate` VARCHAR(45) NULL,
  `exitstatus` INT NULL,
  `reservednode_id` VARCHAR(255) NULL,
  `reservedpriority` INT NULL,
  `reservedmemory` INT NULL,
  `reservedvcores` INT NULL,
  PRIMARY KEY (`containerid_id`, `appattemptid_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(`appattemptid_id`)$$


delimiter $$

CREATE TABLE `yarn_launchedcontainers` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `containerid_id` VARCHAR(45) NOT NULL,
  `rmcontainer_id` VARCHAR(45) NULL,
  PRIMARY KEY (`rmnodeid`, `containerid_id`),
  CONSTRAINT `rmnodeid`
    FOREIGN KEY (`rmnodeid`)
    REFERENCES `yarn_ficascheduler_node` (`rmnodeid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(containerid_id)$$


delimiter $$

CREATE TABLE `yarn_rmnode_finishedapplications` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `applicationid` VARCHAR(45) NOT NULL,
  `pendingeventid` INT,
  PRIMARY KEY (`rmnodeid`, `applicationid`),
  INDEX `index2` (`rmnodeid` ASC),
  CONSTRAINT `rmnodeid`
    FOREIGN KEY (`rmnodeid`)
    REFERENCES `yarn_rmnode` (`rmnodeid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(applicationid) $$


delimiter $$

CREATE TABLE `yarn_schedulerapplication` (
  `appid` VARCHAR(45) NOT NULL,
  `user` VARCHAR(45) NULL,
  `queuename` VARCHAR(45) NULL,
  PRIMARY KEY (`appid`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(appid)$$


delimiter $$



CREATE TABLE `yarn_appschedulinginfo` (
  `applicationattemptid` VARCHAR(45) NOT NULL,
  `appid` VARCHAR(45) NOT NULL,
  `queuename` VARCHAR(45) NULL,
  `user` VARCHAR(251) NULL,
  `containeridcounter` INT NULL,
  `pending` BIT(8) NULL,
  `stoped` BIT(8) NULL,
  `username` varchar(150) NULL,
  `projectname` varchar(100) NULL,
  PRIMARY KEY (`applicationattemptid`),
  INDEX `appid_idx` (`appid` ASC),
  CONSTRAINT `appid`
    FOREIGN KEY (`appid`)
    REFERENCES `yarn_schedulerapplication` (`appid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(applicationattemptid)$$



delimiter $$



CREATE TABLE `yarn_schedulerapp_reservedcontainers` (
  `schedulerapp_id` VARCHAR(45) NOT NULL,
  `priority_id` INT NULL,
  `nodeid` VARCHAR(255) NULL,
  `rmcontainer_id` VARCHAR(45) NULL,
PRIMARY KEY (`schedulerapp_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(`schedulerapp_id`)$$


delimiter $$

CREATE TABLE `yarn_schedulerapp_schedulingopportunities` (
  `schedulerapp_id` VARCHAR(45) NOT NULL,
  `priority_id` INT NULL,
  `counter` INT NULL,
PRIMARY KEY (`schedulerapp_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `yarn_schedulerapp_lastscheduledcontainer` (
  `schedulerapp_id` VARCHAR(45) NOT NULL,
  `priority_id` INT NOT NULL,
  `time` BIGINT NULL,
PRIMARY KEY (`schedulerapp_id`, `priority_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$



CREATE TABLE `yarn_appschedulinginfo_blacklist` (
  `applicationattemptid` VARCHAR(45) NOT NULL,
  `blacklisted` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`applicationattemptid`, `blacklisted`),
  CONSTRAINT `applicationattemptid`
    FOREIGN KEY (`applicationattemptid`)
    REFERENCES `yarn_appschedulinginfo` (`applicationattemptid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `yarn_container` (
  `containerid_id` VARCHAR(45) NOT NULL,
  `containerstate` VARBINARY(13500) NULL,
  PRIMARY KEY (`containerid_id`)#,
  #CONSTRAINT `containerid_id`
  #  FOREIGN KEY (`containerid_id`)
  #  REFERENCES `yarn_rmcontainer` (`containerid_id`)
  #  ON DELETE CASCADE
  #  ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(`containerid_id`) $$


delimiter $$



CREATE TABLE `yarn_rmctx_inactivenodes` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`rmnodeid`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(rmnodeid) $$


delimiter $$

CREATE TABLE `yarn_resourcerequest` (
  `applicationattemptid` VARCHAR(45) NOT NULL,
  `priority` INT NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  `resourcerequeststate` VARBINARY(13500) NULL,
  PRIMARY KEY (`applicationattemptid`, `priority`, `name`)#,
#  CONSTRAINT `applicationattemptid`
#    FOREIGN KEY (`applicationattemptid`)
#    REFERENCES `yarn_appschedulinginfo` (`applicationattemptid`)
#    ON DELETE NO action
#    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(`applicationattemptid`)$$


delimiter $$

CREATE TABLE `yarn_fsscheduler_node` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `numcontainers` INT NULL,
  `reservedcontainer_id` VARCHAR(45) NULL,
  `reservedappschedulable_id` VARCHAR(45) NULL,
PRIMARY KEY (`rmnodeid`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$



CREATE TABLE `yarn_secret_manager_keys` (
  `id` VARCHAR(255) NOT NULL,
  `key` VARBINARY(13500) NULL,
PRIMARY KEY (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$


delimiter $$

CREATE TABLE `yarn_allocate_response` (
  `applicationattemptid` VARCHAR(45) NOT NULL,
  `allocate_response` VARBINARY(13500) NULL,
  `responseid` INT NOT NULL,
PRIMARY KEY (`applicationattemptid`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(applicationattemptid)$$

delimiter $$

CREATE TABLE `yarn_allocated_containers` (
  `applicationattemptid` VARCHAR(45) NOT NULL,
 `containerid` VARCHAR(45) NOT NULL,
 `responseid` INT NOT NULL,
PRIMARY KEY (`applicationattemptid`, `containerid`,`responseid`),
INDEX `applicationattemptid` (`applicationattemptid` ASC),
INDEX `responseid` (`responseid` ASC),
CONSTRAINT `appandresponseid`
    FOREIGN KEY (`applicationattemptid`)
    REFERENCES `yarn_allocate_response` (`applicationattemptid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(applicationattemptid)$$

delimiter $$

CREATE TABLE `yarn_completed_containers_status` (
  `applicationattemptid` VARCHAR(45) NOT NULL,
 `containerid` VARCHAR(45) NOT NULL,
 `responseid` INT NOT NULL,
 `status` varbinary(1000),
PRIMARY KEY (`applicationattemptid`, `containerid`,`responseid`),
INDEX `applicationattemptid` (`applicationattemptid` ASC),
INDEX `responseid` (`responseid` ASC),
CONSTRAINT `appandresponseid`
    FOREIGN KEY (`applicationattemptid`)
    REFERENCES `yarn_allocate_response` (`applicationattemptid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(responseid)$$

delimiter $$

CREATE TABLE `yarn_allocated_nmtokens` (
  `applicationattemptid` VARCHAR(45) NOT NULL,
 `nmtoken` VARBINARY(1000) NOT NULL,
  `responseid` INT NOT NULL,
PRIMARY KEY (`applicationattemptid`, `nmtoken`,`responseid`),
CONSTRAINT `applicationattemptid`
    FOREIGN KEY (`applicationattemptid`)
    REFERENCES `yarn_allocate_response` (`applicationattemptid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(applicationattemptid)$$

delimiter $$

CREATE TABLE `yarn_rms_load` (
  `rmhostname` VARCHAR(100) NOT NULL,
  `load` BIGINT NULL,
PRIMARY KEY (`rmhostname`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$

delimiter $$

CREATE TABLE `yarn_nextheartbeat` (
  `rmnodeid` VARCHAR(255) NOT NULL,
  `nextheartbeat` INT NULL,
  `pendingeventid` INT,
  PRIMARY KEY (`rmnodeid`),
  CONSTRAINT `rmnodeid`
    FOREIGN KEY (`rmnodeid`)
    REFERENCES `yarn_rmnode` (`rmnodeid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
)ENGINE = ndbcluster DEFAULT CHARSET=latin1 PARTITION BY KEY(rmnodeid)$$

delimiter $$

CREATE TABLE `yarn_pendingevents` (
  `id` INT NOT NULL,
  `rmnodeid` VARCHAR(255) NOT NULL,
  `type` INT NULL,
  `status` INT NULL,
  `last_hb` INT NULL,
  PRIMARY KEY (`id`, `rmnodeid`))
ENGINE = ndbcluster DEFAULT CHARSET=latin1$$
delimiter $$

CREATE TABLE `hdfs_metadata_log` (
  `dataset_id` int(11) NOT NULL,
  `inode_id` int(11) NOT NULL,
  `timestamp` bigint(20) NOT NULL,
  `inode_pid` int(11) NOT NULL,
  `inode_name` varchar(255) NOT NULL DEFAULT '',
  `operation` smallint(11) NOT NULL,
  PRIMARY KEY (`dataset_id` ,`inode_id` , `timestamp`),
  KEY `timestamp` (`timestamp`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$

delimiter $$

CREATE TABLE `hdfs_inode_dataset_lookup` (
  `inode_id` int(11) NOT NULL,
  `dataset_id` int(11) NOT NULL,
  KEY(`dataset_id`),
  PRIMARY KEY (`inode_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$

delimiter $$

CREATE TABLE `hdfs_access_log` (
  `inode_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `access_time` bigint(20) NOT NULL,
  PRIMARY KEY (`inode_id` , `user_id` , `access_time`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$

delimiter $$

CREATE TABLE `hdfs_size_log` (
  `inode_id` int(11) NOT NULL,
  `size` bigint(20) NOT NULL,
  PRIMARY KEY (`inode_id` , `size`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1$$

delimiter $$

CREATE TABLE `yarn_schedulerapp_reservations` (
  `schedulerapp_id` VARCHAR(45) NOT NULL,
  `priority_id` INT NOT NULL,
  `counter` INT NULL,
  PRIMARY KEY (`schedulerapp_id`, `priority_id`))
ENGINE = ndbcluster$$

delimiter $$

CREATE TABLE `yarn_localitylevel` (
  `schedulerapp_id` VARCHAR(45) NOT NULL,
  `priority_id` INT NOT NULL,
  `nodetype` VARCHAR(45) NULL,
  PRIMARY KEY (`schedulerapp_id`, `priority_id`))
ENGINE = ndbcluster$$


delimiter $$

CREATE TABLE `yarn_appschedulable` (
  `schedulerapp_id` VARCHAR(45) NOT NULL,
  `starttime` BIGINT NULL,
  `fsqueuename` VARCHAR(45) NULL,
  PRIMARY KEY (`schedulerapp_id`))
ENGINE = ndbcluster$$

delimiter $$

CREATE TABLE `yarn_preemptionmap` (
  `schedulerapp_id` VARCHAR(45) NOT NULL,
  `rmcontainer_id` VARCHAR(45) NOT NULL,
  `value` BIGINT NULL,
  PRIMARY KEY (`schedulerapp_id`, `rmcontainer_id`))
ENGINE = ndbcluster$$

delimiter $$

CREATE TABLE `yarn_runnable_apps` (
  `queuename` VARCHAR(45) NOT NULL,
  `schedulerapp_id` VARCHAR(45) NOT NULL,
  `isrunnable` BIT NOT NULL,
  PRIMARY KEY (`queuename`, `schedulerapp_id`))
ENGINE = ndbcluster$$

delimiter $$

CREATE TABLE `yarn_projects_quota` (
  `projectname` VARCHAR(100) NOT NULL,
  `total` FLOAT DEFAULT '0',
  `quota_remaining` FLOAT  DEFAULT '0',
  PRIMARY KEY (`projectname`),
  KEY total_idx(`total`),
  KEY quota_remaining_idx(`quota_remaining`))
ENGINE = ndbcluster PARTITION BY KEY(projectname)$$

delimiter $$

CREATE TABLE `yarn_containers_logs` (
  `container_id` VARCHAR(255) NOT NULL,
  `start` BIGINT NOT NULL,
  `stop` BIGINT  DEFAULT NULL,
  `exit_status` INT  DEFAULT NULL,
  `price` FLOAT  DEFAULT NULL,
  PRIMARY KEY (`container_id`))
ENGINE = ndbcluster $$
#PARTITION BY KEY(container_id)$$

delimiter $$

CREATE TABLE `yarn_projects_daily_cost` (
  `user` VARCHAR(255) NOT NULL,
  `projectname` VARCHAR(100) NOT NULL,
  `day` BIGINT NOT NULL,
  `credits_used` FLOAT  DEFAULT NULL,
  PRIMARY KEY (`projectname`, `day`, `user`))
ENGINE = ndbcluster PARTITION BY KEY(day)$$

delimiter $$

CREATE TABLE `yarn_containers_checkpoint` (
  `container_id` VARCHAR(255) NOT NULL,
  `checkpoint` BIGINT NOT NULL,
  `price` FLOAT NOT NULL,
  PRIMARY KEY (`container_id`))
ENGINE = ndbcluster PARTITION BY KEY(container_id)$$

delimiter $$

CREATE TABLE `yarn_running_price` (
  `id` VARCHAR(255) NOT NULL,
  `time` BIGINT NOT NULL,
  `price` FLOAT NOT NULL,
  PRIMARY KEY (`id`))
ENGINE = ndbcluster$$

delimiter $$

CREATE TABLE `yarn_history_price` (
  `time` BIGINT NOT NULL,
  `price` FLOAT NOT NULL,
  PRIMARY KEY (`time`))
ENGINE = ndbcluster$$

delimiter $$

CREATE TABLE `yarn_heartbeat_rpc` (
  `rpcid` INT NOT NULL,
  `nodeid` VARCHAR(255) NOT NULL,
  `responseid` INT NOT NULL,
  `node_health_status` VARBINARY(1000) NOT NULL,
  `last_container_token_key` VARBINARY(1000) NOT NULL,
  `last_nm_key` VARBINARY(1000) NOT NULL,
  PRIMARY KEY (`rpcid`),
  CONSTRAINT `rpcid`
    FOREIGN KEY (`rpcid`)
    REFERENCES `yarn_appmaster_rpc` (`rpcid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION)
ENGINE = ndbcluster PARTITION BY KEY(rpcid)$$

delimiter $$

CREATE TABLE `yarn_heartbeat_container_statuses` (
  `rpcid` INT NOT NULL,
  `containerid` VARCHAR(200) NOT NULL,
  `status` VARBINARY(3000) NOT NULL,
  PRIMARY KEY (`rpcid`, `containerid`),
  CONSTRAINT `rpcid`
    FOREIGN KEY (`rpcid`)
    REFERENCES `yarn_appmaster_rpc` (`rpcid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION)
ENGINE = ndbcluster PARTITION BY KEY(rpcid)$$

delimiter $$

CREATE TABLE `yarn_heartbeat_keepalive_app` (
  `rpcid` INT NOT NULL,
  `appid` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`rpcid`, `appid`),
  CONSTRAINT `rpcid`
    FOREIGN KEY (`rpcid`)
    REFERENCES `yarn_appmaster_rpc` (`rpcid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION)
ENGINE = ndbcluster PARTITION BY KEY(rpcid)$$

delimiter $$

CREATE TABLE `yarn_allocate_rpc` (
  `rpcid` INT NOT NULL,
  `progress` float NOT NULL,
  `responseid` INT NOT NULL,
  PRIMARY KEY (`rpcid`),
  CONSTRAINT `rpcid`
    FOREIGN KEY (`rpcid`)
    REFERENCES `yarn_appmaster_rpc` (`rpcid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION)
ENGINE = ndbcluster PARTITION BY KEY(rpcid)$$

delimiter $$

CREATE TABLE `yarn_allocate_rpc_ask` (
  `rpcid` INT NOT NULL,
  `requestid` VARCHAR(200) NOT NULL,
  `request` varbinary(1000) NULL,
  PRIMARY KEY (`rpcid`, `requestid`),
  CONSTRAINT `rpcid`
    FOREIGN KEY (`rpcid`)
    REFERENCES `yarn_appmaster_rpc` (`rpcid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION)
ENGINE = ndbcluster PARTITION BY KEY(rpcid)$$

delimiter $$

CREATE TABLE `yarn_allocate_rpc_blacklist_add` (
  `rpcid` INT NOT NULL,
  `resource` VARCHAR(200) NOT NULL,
  PRIMARY KEY (`rpcid`, `resource`),
  CONSTRAINT `rpcid`
    FOREIGN KEY (`rpcid`)
    REFERENCES `yarn_appmaster_rpc` (`rpcid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION)
ENGINE = ndbcluster PARTITION BY KEY(rpcid)$$

delimiter $$

CREATE TABLE `yarn_allocate_rpc_blacklist_remove` (
  `rpcid` INT NOT NULL,
  `resource` VARCHAR(200) NOT NULL,
  PRIMARY KEY (`rpcid`, `resource`),
  CONSTRAINT `rpcid`
    FOREIGN KEY (`rpcid`)
    REFERENCES `yarn_appmaster_rpc` (`rpcid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION)
ENGINE = ndbcluster PARTITION BY KEY(rpcid)$$

delimiter $$

CREATE TABLE `yarn_allocate_rpc_release` (
  `rpcid` INT NOT NULL,
  `containerid` VARCHAR(200) NOT NULL,
  PRIMARY KEY (`rpcid`, `containerid`),
  CONSTRAINT `rpcid`
    FOREIGN KEY (`rpcid`)
    REFERENCES `yarn_appmaster_rpc` (`rpcid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION)
ENGINE = ndbcluster PARTITION BY KEY(rpcid)$$

delimiter $$

CREATE TABLE `yarn_allocate_rpc_resource_increase` (
  `rpcid` INT NOT NULL,
  `containerid` INT NOT NULL,
  `request` varbinary(1000) NULL,
  PRIMARY KEY (`rpcid`, `containerid`),
  CONSTRAINT `rpcid`
    FOREIGN KEY (`rpcid`)
    REFERENCES `yarn_appmaster_rpc` (`rpcid`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION)
ENGINE = ndbcluster PARTITION BY KEY(rpcid)$$

delimiter $$

CREATE TABLE `yarn_cs_leaf_queue_pending_apps` (
  `app_attempt_id` varchar(200) NOT NULL,
  `path` varchar(200) NOT NULL,
  PRIMARY KEY (`app_attempt_id`))
ENGINE = ndbcluster PARTITION BY KEY(app_attempt_id)$$

delimiter $$

CREATE TABLE `yarn_just_finished_containers` (
  `containerid` varchar(200) NOT NULL,
  `appattemptid` varchar(200) NOT NULL,
  `container` varbinary(1000) NOT NULL,
  PRIMARY KEY (`containerid`,`appattemptid`))
ENGINE = ndbcluster PARTITION BY KEY(`appattemptid`)$$
