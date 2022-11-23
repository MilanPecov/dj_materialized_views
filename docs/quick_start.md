# Quick start

## Overview

Two new admin models have been added to the app
![img.png](images/mv_admin_page.png)

All that is left is to:

* create a Materialized View entry
* run the admin action that creates the materialized view in the database and starts the refresh loop

## Create Materialized View Entry
![](images/mv_creation.png)

* `Title` - This entry's title
* `DB Table` - The name of the database table
* `SQL Query` - The query that represents the database table
* `Periodic Task` - How frequently do you want this table to be refreshed? Only three fields are required:
    * `Name` - the task's name
    * `Task (custom)` - this will be linked to the correct celery task automatically
    * `Interval Schedule` - how frequently the task should be run
    ![img.png](images/mv_interval.png)

* `Indexes` -  concurrent refreshing requires at least one index


## Create the Materialized View in the database and start the refresh loop

Select the materialized view entry and execute the admin action `Create Materialized View`
![img.png](images/mv_actions.png)

Here's a detailed explanation of what these admin actions do:

* `Create Materialized View` - creates the materialized view in the database and enables the periodic refresh task
* `Refresh Materialized View` - useful if you want to manually refresh the materialized view
* `Drop Materialized View` - removes the materialized view from the database and disables the periodic refresh task
* `Delete selected Materialized View` - deletes the materialized view from the database and from the admin panel