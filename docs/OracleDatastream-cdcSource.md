# Oracle (by Datastream) CDC Source


Description
-----------
Oracle (by Datastream) CDC Source plugin replicates all row-level changes in Oracle server databases.
This plugin creates new streams or modifies existing streams of [Datastream](https://cloud.google.com/datastream/) to provide source change data for your CDAP or Cloud Data Fusion replication jobs. 
 
Oracle Versions Supported
-----------
Only those Oracle versions that are supported by Datastream are supported by this source plugin. See the Datastream documentation for the [supported versions of Oracle database](https://cloud.google.com/datastream/docs/sources#versionsforsourcedb).

Before You Start
-----------
### Enable the Datastream API
[Enable the Datastream API](https://cloud.google.com/datastream/docs/before-you-begin) for a Google Cloud project you want to use.  
Give the project name to this plugin as the `Project` property.  
Alternatively if you are using Data Fusion, you can leave the `Project` property as default (`auto-detect`), which will default to your Cloud Data Fusion project.    
  
### Grant CDAP the permission to call the Datastream API
- Prepare a service account and give it to this plugin as the `Datastream Service Account Key` property. This service account will be used as the identity to call the Datastream API when you create a replication draft and run the replication jobs.  
- Alternatively, if you are using Data Fusion, you can leave the `Datastream Service Account Key` property as default ("auto-detect") which will default to Data Fusion Service account when you create a replication draft and Dataproc Service Account when you run the replication job.     
Data Fusion Service Account is in the form: `service-customer-project-number@gcp-sa-datafusion.iam.gserviceaccount.com`. This service account is used to call the Datastream API when you create a replication draft.    
Dataproc Service Account is the identity Dataproc will use to run replication jobs. By default it's the default GCE (Google Cloud Engine) service account in form of `customer-project-number-compute@developer.gserviceaccount.com`. When you create a Data Fusion instance, you can also choose a different Dataproc Service Account. This service account is used as the identity to call the Datastream API when replication jobs are run.   

Grant those service accounts the "Datastream Admin" role on the project that enables the Datastream API. For more information, see [Access control for projects using IAM](https://cloud.google.com/resource-manager/docs/access-control-proj).  
For more information about service accounts and service account keys, see [Creating and managing service account keys](https://cloud.google.com/iam/docs/creating-managing-service-account-keys).

### Grant CDAP the permission to access GCS (Google Cloud Storage)
Datastream will write its output to the GCS bucket you specified as the `GCS Bucket` property of this plugin. This bucket can be an existing one or non-existing one (this plugin will create one for you).  
- Prepare a service account and give it to this plugin as the `GCS Service Account Key` property. This service account will be used as the identity to get and create (if you want this plugin to create a new bucket) the bucket and read the Datastream output from that bucket.  
- Alternatively, if you are using Data Fusion, you can leave the `GCS Service Account Key` property as default ("auto-detect") which will default to Data Fusion Service account when you create a replication draft and Dataproc Service Account when you run the replication job.     
Data Fusion Service Account is in form of `service-customer-project-number@gcp-sa-datafusion.iam.gserviceaccount.com`. This service account is used as the identity to get and create (if you want this plugin to create a new bucket) the bucket when you create a replication draft.      
Dataproc Service Account is the identity Dataproc will use to run replication jobs. By default it's the default GCE (Google Cloud Engine) service account in form of `customer-project-number-compute@developer.gserviceaccount.com`. When you create a Data Fusion instance, you can also choose a different Dataproc Service Account. This service account is used as the identity to  get and create (if you want this plugin to create a new bucket) the bucket and read the Datastream result from that bucket.

Grant the following permissions to those service accounts on the project where the bucket is:
- `storage.buckets.get`
- `storage.buckets.create` (only if you want this plugin to create a new bucket for you)
- `storage.objects.list`
- `storage.objects.get`

To grant the above permissions, you need to 
[create a custom role](https://cloud.google.com/iam/docs/creating-custom-roles#creating_a_custom_role) 
that contains above permissions and grant that role to those service accounts. 
See [Access control for projects using IAM](https://cloud.google.com/resource-manager/docs/access-control-proj) for details.  
For more information about service accounts and service account keys,
see [Creating and managing service account keys](https://cloud.google.com/iam/docs/creating-managing-service-account-keys).  
Note, You don't need to grant above permissions to those service accounts if you have already granted "Datastream Admin" role to them.  

### Configure Oracle Database
See [Configure your source Oracle database](https://cloud.google.com/datastream/docs/configure-your-source-database).

Plugin properties
-----------

**Use an existing Datastream stream**: Whether to let this plugin create a new Datastream stream or use an existing Datastream stream. It's recommended that you only have one Datastream stream per Oracle Database for performance concerns.

**Region**: The region of the Datastream stream. Supported regions can be found [here](https://cloud.google.com/datastream/docs/ip-allowlists-and-regions).

**Stream ID**: The ID of the existing Datastream stream. Only applicable when you want to use an existing Datastream stream.

**Connectivity Method**: How you want the Datastream to connect to your database. See [Source network connectivity options](https://cloud.google.com/datastream/docs/source-network-connectivity-options) about what each option means and what you need to do for your network settings.

**Host**(in the **Basic** section): The hostname or IP address of your SSH tunnel bastion server. Only applicalbe when you choose `Forward SSH Tunnel` as your `Connectivity Method`.

**Port**(in the **Basic** section): The port number to use to connect to your SSH tunnel bastion server. Only applicable when you choose `Forward SSH Tunnel` as your `Connectivity Method`.

**Username**(in the **Basic** section): The username that Datastream can use to connect to your SSH tunnel bastion server. Only applicable when you choose `Forward SSH Tunnel` as your `Connectivity Method`.

**Authentication Method**: How your SSH tunnel bastion server authenticates the user (Datastream). Only applicable when you choose `Forward SSH Tunnel` as your `Connectivity Method`.

**Password**(in the **Basic** section): The passowrd to use to connect to your SSH tunnel bastion server. Only applicable when you choose `Password` as your `Authentication Method`.

**Private Key**: The private key Datastream will use to connect to your SSH tunnel bastion server that matches the public key assigned to it. Only applicable when you choose `Private/Public Key Pair` as your `Authentication Method`.

**Private Connection Name**: Name of the private connection. The network admins of the Google Cloud Platform project should create a VPC peering between the database VPC and the Datastream VPC (see [Use private connectivity](https://cloud.google.com/datastream/docs/source-network-connectivity-options#privateconnectivity) for details). THis is the name of the VPC peering they created. Only applicable when you choose `Private connectivity (VPC peering)` as your `Connectivity Method`.

**Host**(in the **Database Location** section): Hostname or IP address of your Oracle server to read from.

**Port**: Port number to use to connect to your Oracle server.

**System Identifier (SID)**: The system identifier(SID) of the oracle database you want to replicate data from.

**Username**: Username to use to connect to your Oracle server. 

**Password**: Password to use to connect to your Oracle server.

**Replicate Existing Data**: Whether to replicate existing data from the source database. When false, any existing data in the source tables will be ignored, and only changes that happened after the pipeline started will be replicated. By default, existing data will be replicated. 

**Project**: The Google Cloud Platform project that has enabled the Datastream API. It will default to the value of the system property `GOOGLE_CLOUD_PROJECT` or `GCLOUD_PROJECT` for CDAP and Data Fusion Project for Data Fusion. 

**Datastream Service Account Key**: The service account key for the service account that will be used as the identity to call the Datastream API. It will default to the content of the file referred by the system property `GOOGLE_APPLICATION_CREDENTIALS` for CDAP and Data Fusion Service Account for Data Fusion.

**GCS Service Account Key**: The service account key for the service account that will be used as the identity to access GCS. Datastream will write the change stream to the GCS bucket you specified as the `GCS Bucket` property of this plugin. This service account will be used as the identity to get and create (if you want this plugin to create a new bucket) the bucket and read Datastream result from the bucket. It will default to the content of the file referred by the system property `GOOGLE_APPLICATION_CREDENTIALS` for CDAP and Data Fusion Service Account for Data Fusion. 

**GCS Bucket**: The GCS (Google Cloud Storage) bucket that Datastream will write its output to. If the bucket you provide doesn't exist or you leave it as empty, this plugin will create a new one in the `Project` you specified in this plugin. 

**Path Prefix**: The GCS (Google Cloud Storage) path prefix in the bucket that Datastream will write its output to. This prefix will be prefixed to the Datastream output path. It's usually used when you want Datastream to write its output to an existing bucket and you want to easily differentiate it from other existing GCS files by its path prefix.  

Limitations
-----------

### Primary Key Update 
Each row is identified by the Primary key of the table. Upon a Primary key update on the oracle source, the data stream generates 2 events : Insert + delete. 
Since Events written by Datastream may arrive out of order , we do not hard delete and rather mark the column `_is_deleted` as true when we get a Delete event. So, in case of a Primary key update, we will have the row with new value inserted while the old row will be present but would be marked as deleted.

### Multiple updates to the same row
Events written by Datastream may arrive out of order. BigQuery plugin cannot sort the events if multiple events happened in the same milli-second to the same row. The consequence is that target database may apply those events in the order of receiving them. But such case is very rare becasue it only happens when multiple change events against the same row are committed in the same millisecond and Datastream write them out of order.

Trouble Shooting changes got replicated with a high latency
-----------
The following issue occurs when you have incorrect redo log settings on your Oracle database:
After you make some changes and don't see them got replicated for quite long time, or those changes get replicated with a high latency.
To resolve this issue, make sure you have the correct configuration of redo log settings by following [Work with Oracle database redo log files](https://cloud.google.com/datastream/docs/work-with-oracle-database-redo-log-files). Datastream relies on archived Oracle redo log to stream changes, if the redo log was not rotated (archived) , the changes in that log could not be streamed.
