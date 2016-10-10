CREATE EXTERNAL TABLE hospitals5
(provider_id string,
name string,
address string,
city string,
state string,
zip string,
county string,
phone string,
type string,
ownership string,
er_dummy string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\' )
STORED AS TEXTFILE
LOCATION "/user/w205/hospital_compare/hospitals";

CREATE EXTERNAL TABLE effective_care5
(provider_id string,
name string,
address string,
city string,
state string,
zip string,
county string,
phone string,
condition string,
measure_id string,
measure_name string,
score int,
sample int,
footnote string,
measure_start string,
measure_end string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\' )
STORED AS TEXTFILE
LOCATION "/user/w205/hospital_compare/effective_care";


CREATE EXTERNAL TABLE readmissions5
(provider_id string,
name string,
address string,
city string,
state string,
zip string,
county string,
phone string,
measure_name string,
measure_id string,
relative_usa string,
denominator int,
score float,
sample int,
estimate_low float,
estimate_high float,
footnote string,
measure_start string,
measure_end string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\' )
STORED AS TEXTFILE
LOCATION "/user/w205/hospital_compare/readmissions";

CREATE EXTERNAL TABLE Measures5
(measure_name string,
measure_id string,
measure_qstart string,
measure_start string,
measure_qend string,
measure_end string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\' )
STORED AS TEXTFILE
LOCATION "/user/w205/hospital_compare/Measures";

CREATE EXTERNAL TABLE survey_responses5
(provider_id string,
name string,
address string,
city string,
state string,
zip string,
county string,
communication_with_nurses_achievement_points string,
communication_with_nurses_improvement_points string,
communication_with_nurses_dimension_score string,
communication_with_doctors_achievement_points string,
communication_with_doctors_improvement_points string,
communication_with_doctors_dimension_score string,
responsiveness_of_hospital_staff_achievement_points string,
responsiveness_of_hospital_staff_improvement_points string,
responsiveness_of_hospital_staff_dimension_score string,
pain_management_achievement_points string,
pain_management_improvement_points string,
pain_management_dimension_score string,
communication_about_medicines_achievement_points string,
communication_about_medicines_improvement_points string,
communication_about_medicines_dimension_score string,
cleanliness_and_quietness_of_hospital_environment_achievement_points string,
cleanliness_and_quietness_of_hospital_environment_improvement_points string,
cleanliness_and_quietness_of_hospital_environment_dimension_score string,
discharge_information_achievement_points string,
discharge_information_improvement_points string,
discharge_information_dimension_score string,
overall_rating_of_hospital_achievement_points string,
overall_rating_of_hospital_improvement_points string,
overall_rating_of_hospital_dimension_score string,
hcahps_base_score int,
hcahps_consistency_score int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\' )
STORED AS TEXTFILE
LOCATION "/user/w205/hospital_compare/survey_responses";
