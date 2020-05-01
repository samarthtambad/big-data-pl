package analytics

class Analyze(var path: String) {
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

    // 2D aggregation - num projects per language per year
    val numProjects = projectsDF.rollup("year", "language").agg(count("language") as "count").sort($"count".desc)
}
