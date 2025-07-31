## Datastream Delta Plugin for CDAP Delta Application

The plugin uses GCP Datastream service to generate change events of Oracle DB.

Tests require an actual connection to GCP. To run the tests, set the following system properties:  
  -Dproject.id=[GCP project id]  
  Alternatively, you can set -DGCLOUD_PROJECT or -DGOOGLE_CLOUD_PROJECT for GCP project id.  
  -Dservice.account.file=[path to service account key file]  
  -Dservice.location=[region of the Datastream instance]  
  -Doracle.host=[Oracle DB ip address or hostname]  
  -Doracle.user=[Oracle DB username]  
  -Doracle.password=[Oracle DB password]
  -Doracle.database=[Oracle DB SID]  
  -Dgcs.bucket=[GCSBucket Name for writing Datastream result]  
  -Doracle.tables=[List of tables to be replicated, separated by comma]  
  -Dstream.id =[Id of existing Datastream stream id]  
  
The GCP project should have Datastream API enabled.

The service account will need permission to :
  create/delete/get Datastream connection profiles
  create/delete/get Datastream streams
  create/read from GCS Bucket

The Oracle instance should be configured according to the Datastream requirements - 
https://cloud.google.com/datastream/docs/configure-your-source-database

The GCS bucket should be in the same region as Datastream instance and Datastream Service Account
should have the permission to write to this bucket.  
