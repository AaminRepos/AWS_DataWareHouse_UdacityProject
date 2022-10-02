In This project the music streaming start up sparkify, and have grown to where a move he cloud on AWS is needed to move their data and their processes from the S3 storage bucket on AWS to a AWS Redshift cluster and transfrom the data to fit their analytical needs.

This proeject consists of 4 other files, 'create_tables', 'etl', 'sql_queries' and these three files are all python files to be run with python cmd or any other python running terminal.

Each file does the following:

1-sql_queries.py- Since this data is coming from 2 different sources.
The event logging from the app and the song's data, here is where we will take insert them into 2 staging tables then finally move the data to our final 5, 1 fact vs 4 dimention relational database design.

2-create_tables.py- we run this file first with python and it will call up on creating our tables we have designed in our sql_quries file into our redshift cluster.

3-etl.py- This will be the second the file we run with python, this will copy the data from sparkify's bucket into our staging tables and then into our database tables, prepared for queires and analytics.

4-dwh.cfg- This file contains the credntial for the end point and the user on AWS that all 3 files use and refer back to, to connect to sparkify's S3 bucket, copy the data from the thet storage bucket into our cluster and creating the database.

Here we used the fact vs dimention star schema relational database with will allow for quick mass aggregation and the ability to use JOINs to extract or filter by further data points and types for both the songs and their app's users.
(songplays)
Below is the schema deigned and advised by udacity:

-Fact Table- (songplays)
coloumns = (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

-Dimension Tables-

users - users in the app
coloumns = (user_id, first_name, last_name, gender, level)

songs - songs in music database
coloumns = (song_id, title, artist_id, year, duration)

artists - artists in music database
coloumns = (artist_id, name, location, lattitude, longitude)

time - timestamps of records in songplays broken down into specific units
coloumns = (start_time, hour, day, week, month, year, weekday)



refrences:
https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

https://aws.amazon.com/blogs/big-data/automate-amazon-redshift-cluster-creation-using-aws-cloudformation/

https://docs.python.org/3/library/configparser.html

https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html