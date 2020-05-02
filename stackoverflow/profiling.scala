val posts_count = df.count()
val questions_count = df.filter(df("PostTypeId") === 1).count()
val answers_count = df.filter(df("PostTypeId") === 1).count()
