/* 
Analytics on data to generate insights

Assume following DFs already loaded in interactive mode

usersDF
commitsDF
pullRequestsDF
projectsDF
projectLanguagesDF
*/

val joinedCommitsLangData = commitsDF.join(projectLanguagesDF, Seq("project_id"), "inner")
val df_countCommitLangData = sqlContext.sql("""SELECT year, COUNT(language) FROM joinedCommitsLangData GROUP BY year""")
val df_countProjectLang = sqlContext.sql("""SELECT year, COUNT(language) FROM projectLanguagesData GROUP BY year""")
