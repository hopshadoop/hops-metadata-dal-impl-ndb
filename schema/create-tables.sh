mysql --host=$1 --port=$2 -u $3 -p$4 $5 < schema.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_2.8.2.1_to_2.8.2.2.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_2.8.2.2_to_2.8.2.3.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_2.8.2.3_to_2.8.2.4.sql &&
mysql --host=$1 --port=$2 -u $3 -p$4 $5 < update-schema_2.8.2.4_to_2.8.2.5.sql
