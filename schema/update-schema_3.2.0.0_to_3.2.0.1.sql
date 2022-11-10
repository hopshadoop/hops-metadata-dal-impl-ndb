TRUNCATE TABLE hdfs_le_descriptors;

TRUNCATE TABLE yarn_le_descriptors;

ALTER TABLE yarn_applicationattemptstate MODIFY COLUMN applicationattempttrakingurl VARCHAR(300) DEFAULT NULL
