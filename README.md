DataStream Delta Plugin for CDAP Delta Application

It invokes GCP DataStream service to generate change events of Oracle DB.

Tests require an actual connection to GCP. To run the tests, set the following system properties:
  -Dproject.id=[GCP project id]
  Alternatively, you can set -DGCLOUD_PROJECT or -DGOOGLE_CLOUD_PROJECT for GCP project id.
  -Dservice.account.file=[path to service account key file]
  -Dservice.location=[region of the DataStream instance]
  -Doracle.host=[Oracle DB ip address or hostname]
  -Doracle.user=[Oracle DB username]
  -Doracle.password=[Oracle DB password]
  -Doracle.database=[Oracle DB SID]
  -Dgcs.bucket=[GCSBucket Name for writing DataStream result]
  
The GCP project should already enabled DataStream API.

The service account will need permission to :
  create/delete/get DataStream connection profiles
  create/delete/get DataStream streams
  read from GCS Bucket

The Oracle instance should be configured according to the requirement of DataStream:
https://cloud.google.com/datastream/docs/configure-your-source-database

The GCS bucket should be in the same region as DataStream instance and DataStream Service Account
should have the permission to write to this bucket.  
