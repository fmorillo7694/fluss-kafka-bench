# Part-2 reuses the Part-1 scenario SQL. 90_state_comparison.sql is copied here;
# point the table DDL at S3-backed Fluss (datalake.enabled, warehouse=s3://...).
# Iceberg datalake tables need a SINGLE-field PK (Part-1 finding).
