
# Brainiac5 Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
 
## [Unreleased] - 2024-03-31
 
### Changed
  1. Query fucntions were refactored into a class object to improve reusability and mkae the code more readable.

  2. charbuff arugument in CreateTable set to 0 by default. This was done mainly because having a 0 default helps with debugin all individual columns and users may not want a buffer for all columns or a buffer at all.

  3. Primary Key constraint was set to clustered by defualt and ClusterPK aruguement added to turn off clustering for primary key constraint.