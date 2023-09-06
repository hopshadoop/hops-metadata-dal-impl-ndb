mysql --host=$1 --port=$2 -u $3 -p$4 $5 < schema.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_2.8.2.1_to_2.8.2.2.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_2.8.2.2_to_2.8.2.3.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_2.8.2.3_to_2.8.2.4.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_2.8.2.4_to_2.8.2.5.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_2.8.2.5_to_2.8.2.6.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_2.8.2.6_to_2.8.2.7.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_2.8.2.7_to_2.8.2.8.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_2.8.2.8_to_2.8.2.9.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_2.8.2.9_to_2.8.2.10.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_2.8.2.10_to_3.2.0.0.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_3.2.0.0_to_3.2.0.1.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_3.2.0.1_to_3.2.0.2.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_3.2.0.2_to_3.2.0.3.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_3.2.0.3_to_3.2.0.4.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_3.2.0.4_to_3.2.0.5.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_3.2.0.6_to_3.2.0.7.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_3.2.0.7_to_3.2.0.8.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_3.2.0.8_to_3.2.0.9.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_3.2.0.9_to_3.2.0.10.sql&&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_3.2.0.10_to_3.2.0.11.sql&&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_3.2.0.11_to_3.2.0.12.sql
