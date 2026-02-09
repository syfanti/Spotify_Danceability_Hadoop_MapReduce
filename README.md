# Spotify_Danceability_Hadoop_MapReduce
This is a homework for Large Scale Data Management 2026 in AI Master's program of AUEB


Develop your own map-reduce application. 
You will firstdownload and go through your input file, with statistics about spotify songs:

This is a compressed .csv file and you are going to use only some of its columns in your project.
The .csv has a header (first line) which will help you identify the information you need. Your
job is to produce a result file that provides for every country and month pair (for example: “GR:
2024-01”) the most “danceable” song, along with its “danceability” and the average danceability of
all songs for the same country and month pair (for example: “Tacata (Remix): 0.922, avg: 0.454).
While developing your solution keep in mind the following:
2
• You have to come up with appropriate data types for your intermediate and final results and
use them in your classes.
• A combiner function should be used if your reducer actions are commutative and associative.
Otherwise, the combiner function should be disabled.
• Your application should ignore the .csv header.
• Csv files use ‘,’ as a separator. However when this character is used inside double quotes it
should be escaped. The following one-liner1 will help you obtain the fields of a line:
String[] tokens = input.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
To successfully complete the second part of this project you have to provide the code of your application,
i.e., two Java files for the Driver and Map/Reduce classes. You should provide comments
in your map and reduce functions so that their operations are crystal-clear.
