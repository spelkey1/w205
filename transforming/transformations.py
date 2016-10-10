# imports
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
from pyspark.sql import functions as F

###### Create aggregated dfs of good and bad measures and combine into one table ######
good_measures = sqlContext.sql("select * From effective_care5")
bad_measures = sqlContext.sql("select * from readmissions5")

#drop strings
good_measures = good_measures.withColumn('good_score',good_measures.score * 1) 
bad_measures = bad_measures.withColumn('bad_score',bad_measures.score * 1)

# drop measures that aren't percent. Don't have time to go through one by one
good_measures = good_measures[good_measures.good_score <= 100]

# compute mean by hospital. spark 1.5 does not have std!
good_measures_grouped = good_measures.groupBy("provider_id").agg({"good_score":"avg"})
bad_measures_grouped = bad_measures.groupBy("provider_id").agg({"bad_score":"avg"})

# merge dataframes and compute a composite hospital score
hospital_summary = good_measures_grouped.join(bad_measures_grouped, good_measures_grouped.provider_id == bad_measures_grouped.provider_id).drop(bad_measures_grouped.provider_id)
hospital_summary = hospital_summary.withColumnRenamed('avg(good_score)','good_score')
hospital_summary = hospital_summary.withColumnRenamed('avg(bad_score)','bad_score')
hospital_summary = hospital_summary.withColumn('composite_score',hospital_summary.good_score / hospital_summary.bad_score)
hospital_list = sqlContext.sql("select * From hospitals5")
hospital_limited = hospital_list.select('provider_id','name','state')
hospital_summary = hospital_limited.join(hospital_summary, hospital_limited.provider_id == hospital_summary.provider_id).drop(hospital_summary.provider_id)

# move to analysis file when figure out how to export
hospital_summary.sort(hospital_summary.composite_score.desc())

####### Create a state groupby table ######
state_summary = hospital_summary.groupBy('state').agg({"composite_score":"avg", "good_score":"avg", "bad_score":"avg"})
state_summary = state_summary.withColumnRenamed('avg(good_score)','good_score')
state_summary = state_summary.withColumnRenamed('avg(bad_score)','bad_score')
state_summary = state_summary.withColumnRenamed('avg(composite_score)','composite_score')

# move to analysis file when figure out how to export
state_summary.sort(state_summary.composite_score.desc())

####### Create table with average and std by effective care measure #####
# since pyspark 1.5 does not have handy stdv function, have to do it the old-fashioned way

# compute average score per measure and reload into dataframe
good_measures = good_measures.withColumn('numeric_score',good_measures.score * 1)
good_measures_byid = good_measures.groupBy("measure_id").agg({"numeric_score":"avg"})
good_measures_byid = good_measures_byid.withColumnRenamed('avg(numeric_score)','good_score_meas_avg')
good_measures_byid= good_measures_byid.dropna()
measure_var = good_measures.select('measure_id','numeric_score','measure_name')
measure_var.join(good_measures_byid, measure_var.measure_id == good_measures_byid.measure_id, 'left_outer').drop(measure_var.measure_id)
measure_var = measure_var.dropna()


# compute distance from mean for each record
measure_var = measure_var.withColumn('variance', measure_var.numeric_score - measure_var.good_score_meas_avg)
measure_var = measure_var.withColumn('variancesq',F.abs(measure_var.variance))

# compute average distance from mean for each measure and join with average measure table
good_measures_sd = measure_var.groupBy("measure_id").agg({"variancesq":"avg"})
good_measures_sd = good_measures_sd.withColumnRenamed('avg(variancesq)','variance')
#good_measures_byid2 = good_measures_byid.join(good_measures_sd, good_measures_byid.measure_id == good_measures_sd.measure_id).drop(good_measures_sd.measure_id)


# move to analysis file when figure out how to export
good_measures_sd.sort(good_measures_sd.variance.desc())

######## Set up survey correlation
# import table and select relevant variables
survey = sqlContext.sql("select * from survey_responses5")
survey_overall = survey.select("provider_id","overall_rating_of_hospital_dimension_score","overall_rating_of_hospital_improvement_points","overall_rating_of_hospital_achievement_points")
survey_overall = survey_overall.withColumnRenamed('overall_rating_of_hospital_dimension_score','dimension')
survey_overall = survey_overall.withColumnRenamed('overall_rating_of_hospital_improvement_points','improvement')
survey_overall = survey_overall.withColumnRenamed('overall_rating_of_hospital_achievement_points','achievement')

# turn ratings into numeric variables
survey_overall = survey_overall.withColumn('achievment2', survey_overall.achievement[0:2])
survey_overall = survey_overall.withColumn('improvement2', survey_overall.improvement[0:2])
survey_overall = survey_overall.withColumn('dimension2', survey_overall.dimension[0:2])
survey_overall = survey_overall.withColumn('achievement_n',survey_overall.achievment2 * 1)
survey_overall = survey_overall.withColumn('improvement_n',survey_overall.improvement2 * 1)
survey_overall = survey_overall.withColumn('dimension_n',survey_overall.dimension2 * 1)
survey_overall = survey_overall.select("provider_id","achievement_n","dimension_n","improvement_n")
hospital_corr = hospital_summary.join(survey_overall, hospital_summary.provider_id == survey_overall.provider_id, 'left_outer').drop(survey_overall.provider_id)
hospital_corr=hospital_corr.dropna()
hospital_corr.corr("composite_score","achievement_n")
hospital_corr.corr("composite_score","improvement_n")
