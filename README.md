#  SDE Maintenance Script

Script for maintaining enterprise geodatabase (SDE) performance and integrity. All configurations, including sensitive usernames and passwords stored in a separate configuration file. 

Stops any ArcGIS services relying on the database to allow greater state compression.
Reconciles versions.
Compresses GDB.
Rebuilds indexes.
Rebuilds statistics. 

Saves a detailed log file with progress and any errors encountered.
Emails designated admins upon completion with a dynamically constructed HTML body email, allowing success/warnings/error to be easily identified.
