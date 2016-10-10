hdfs dfs -mkdir /user/w205/hospital_compare
cp "loading_and_modeling/raw_data/Timely and Effective Care - Hospital.csv" loading_and_modeling/useful_data/effective_care.csv
cp "loading_and_modeling/raw_data/Readmissions and Deaths - Hospital.csv" loading_and_modeling/useful_data/readmissions.csv
cp "loading_and_modeling/raw_data/Measure Dates.csv" loading_and_modeling/useful_data/Measures.csv
cp "loading_and_modeling/raw_data/hvbp_hcahps_05_28_2015.csv" loading_and_modeling/useful_data/surveys_responses.csv
sed -i 1d loading_and_modeling/useful_data/hospitals.csv
sed -i 1d loading_and_modeling/useful_data/effective_care.csv
sed -i 1d loading_and_modeling/useful_data/readmissions.csv
sed -i 1d loading_and_modeling/useful_data/Measures.csv
sed -i 1d loading_and_modeling/useful_data/surveys_responses.csv
hdfs dfs -put loading_and_modeling/useful_data/hospitals.csv /user/w205/hospital_compare/hospitals
hdfs dfs -put loading_and_modeling/useful_data/effective_care.csv /user/w205/hospital_compare/effective_care
hdfs dfs -put loading_and_modeling/useful_data/readmissions.csv /user/w205/hospital_compare/readmissions
hdfs dfs -put loading_and_modeling/useful_data/Measures.csv /user/w205/hospital_compare/Measures
hdfs dfs -put loading_and_modeling/useful_data/surveys_responses.csv /user/w205/hospital_compare/survey_responses
