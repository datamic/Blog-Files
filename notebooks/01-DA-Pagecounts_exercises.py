# Databricks notebook source
# MAGIC %md ![Wikipedia/Spark Logo](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/wiki_spark.png)
# MAGIC 
# MAGIC #### Analyzing Traffic Patterns to Wikimedia Projects
# MAGIC 
# MAGIC **Objective:**
# MAGIC Study traffic patterns to all English Wikimedia projects from the past hour
# MAGIC 
# MAGIC **Data Source:**
# MAGIC Last hour's English Projects Pagecounts (~35 MB compressed parquet file)
# MAGIC 
# MAGIC **Business Questions:**
# MAGIC 
# MAGIC * Question # 1) How many different English Wikimedia projects saw traffic in the past hour?
# MAGIC * Question # 2) How much traffic did each English Wikimedia project get in the past hour?
# MAGIC * Question # 3) What were the 25 most popular English articles in the past hour?
# MAGIC * Question # 4) How many requests did the "Apache Spark" article recieve during this hour?
# MAGIC * Question # 5) Which Apache project received the most requests during this hour?
# MAGIC * Question # 6) What percentage of the 5.1 million English articles were requested in the past hour?
# MAGIC * Question # 7) How many total requests were there to English Wikipedia Desktop edition in the past hour?
# MAGIC * Question # 8) How many total requests were there to English Wikimedia edition in the past hour?
# MAGIC 
# MAGIC **Technical Accomplishments:**
# MAGIC - Create a `DataFrame`
# MAGIC - Print the schema of a `DataFrame`
# MAGIC - Use the following Transformations: `select()`, `distinct()`, `groupBy()`, `sum()`, `orderBy()`, `filter()`, `limit()`
# MAGIC - Use the following Actions: `show()`, `count()`
# MAGIC - Learn about Wikipedia Namespaces
# MAGIC 
# MAGIC **NOTE**
# MAGIC Please run this notebook in a Spark 2.0 cluster.

# COMMAND ----------

# MAGIC %md ####![Wikipedia Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_wikipedia_tiny.png) **Introduction: Wikipedia Pagecounts**

# COMMAND ----------

# MAGIC %md Until August, 2016, the Wikimedia Foundation poublished hourly page count statistics for all Wikimedia projects and languages. The projects include Wikipedia, Wikibooks, Wikitionary, Wikinews, etc. They elected to
# MAGIC <a href="https://lists.wikimedia.org/pipermail/analytics/2016-March/005060.html" target="_blank">stop publishing that data</a> because it "does not count access to the
# MAGIC mobile site, it does not filter out spider or bot traffic, and it suffers from unknown loss due to logging infrastructure limitations."
# MAGIC 
# MAGIC However, the historical files are still out there, and they still make for an interesting use case. We'll be using the files from August 5, 2016 at 12:00 PM UTC. We've preloaded that data and converted it to a Parquet file for easy consumption.
# MAGIC 
# MAGIC You can see the hourly dump files <a href="https://dumps.wikimedia.org/other/pagecounts-raw/" target="_blank">here</a>.

# COMMAND ----------

# MAGIC %md Each line in the pagecounts files contains 4 fields:
# MAGIC - Project name
# MAGIC - Page title
# MAGIC - Number of requests the page recieved this hour
# MAGIC - Total size in bytes of the content returned
# MAGIC 
# MAGIC |Project Name|Page Title|Requests/Hour|Total Bytes|
# MAGIC |------------|----------|-------------|-----------|
# MAGIC |en|Main_Page|245839|4737756101|
# MAGIC |en|Apache_Spark|1370|29484844|
# MAGIC |en.mw|en|4277001|115533016292|
# MAGIC |en.d|discombobulate|200|284834|
# MAGIC |en.b|US_History|35|7482942|
# MAGIC |fr|Champs-Elysees|344|5609204|

# COMMAND ----------

# MAGIC %md In each line, the first column (such as **en** or **en.b**) is the Wiki**media** project name.
# MAGIC 
# MAGIC Projects without a suffix (a period and subsequent characters) are Wiki**pedia** projects.
# MAGIC 
# MAGIC Projects with a suffix are langauges specific projects, for example:
# MAGIC 
# MAGIC | Suffix | Project          |
# MAGIC |:------ |:---------------- |
# MAGIC | .mw    | wikipedia mobile |
# MAGIC | .d     | wiktionary       |
# MAGIC | .b     | wikibooks        |
# MAGIC | .m     | wikimedia        |
# MAGIC | .n     | wikinews         |
# MAGIC | .q     | wikiquote        |
# MAGIC | .s     | wikisource       |
# MAGIC | .v     | wikiversity      |
# MAGIC | .w     | mediawiki        |
# MAGIC 
# MAGIC So, any line starting with the column **en** refers to the English language Wikipedia (and can be requests from either a mobile or desktop client).
# MAGIC 
# MAGIC **en.d** refers to English language Wiktionary.
# MAGIC 
# MAGIC **fr** is French Wikipedia.
# MAGIC 
# MAGIC There are over 290 language possibilities.

# COMMAND ----------

# MAGIC %md Let's take a look at what our files look like...

# COMMAND ----------

# MAGIC %fs ls /mnt/wikipedia-readonly/pagecounts/staging_parquet_en_only/

# COMMAND ----------

# MAGIC %md To do the same thing programatically...

# COMMAND ----------

display(
  dbutils.fs.ls("/mnt/wikipedia-readonly/pagecounts/staging_parquet_en_only/")
)

# COMMAND ----------

# MAGIC %md ####![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_spark_tiny.png) **Entry Point**

# COMMAND ----------

# MAGIC %md In Spark 1.x, the main entry point was the `SparkContext`.

# COMMAND ----------

sc

# COMMAND ----------

# MAGIC %md In Spark 1.x, the main entry point for Spark SQL was the `SQLContext` and its subclass `HiveContext`.

# COMMAND ----------

sqlContext

# COMMAND ----------

# MAGIC %md In Spark 2.x, both `SparkContext` and `SQLContext` have be superseded by `SparkSession`.

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md ####![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_spark_tiny.png) **Create a DataFrame**

# COMMAND ----------

# MAGIC %md Let's use the `spark` entry point to create a `DataFrame` from the most recent pagecounts file:

# COMMAND ----------

pagecounts_en_all_df = spark.read.parquet('/mnt/wikipedia-readonly/pagecounts/staging_parquet_en_only/')

# COMMAND ----------

# MAGIC %md Look at the first few records in the `DataFrame`:

# COMMAND ----------

pagecounts_en_all_df.show()

# COMMAND ----------

# MAGIC %md `printSchema()` prints out the schema for the DataFrame, the data types for each column and whether a column can be null:

# COMMAND ----------

pagecounts_en_all_df.printSchema()

# COMMAND ----------

# MAGIC %md Notice above that the first 2 columns are typed as `string`, but the requests column holds an `integer` and the bytes_served column holds a `long`.

# COMMAND ----------

# MAGIC %md Count the number of total records (rows) in the DataFrame:

# COMMAND ----------

pagecounts_en_all_df.count()

# COMMAND ----------

# MAGIC %md So, there are between 2 - 3 million rows in the DataFrame. This includes traffic to not just English Wikipedia articles, but also possibly English Wiktionary, Wikibooks, Wikinews, etc.

# COMMAND ----------

# MAGIC %md ####![Wikipedia + Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/wiki_spark_small.png) **Q-1) How many different English Wikimedia projects saw traffic during that hour?**

# COMMAND ----------

# MAGIC %md 
# MAGIC Let's start by creating a `DataFrame` that contains just the one column, **project**.

# COMMAND ----------

(pagecounts_en_all_df.
   select("project").
   show()
 )

# COMMAND ----------

# MAGIC %md Building on our last example, we can now introduce the `distinct()` transformation, reducing the result to a collection of distinct Wikipedia projects names.

# COMMAND ----------

(pagecounts_en_all_df.
   select('project').
   distinct().
   show()
 )

# COMMAND ----------

# MAGIC %md Closer, but you can see that `show()` is `only showing top 20 rows`.
# MAGIC 
# MAGIC Let's fix this, but how?
# MAGIC 
# MAGIC That is, how can we tell `show()` to display more lines?

# COMMAND ----------

# MAGIC %md 
# MAGIC **Challenge 1:**
# MAGIC 0. Open the documentation in another tab for quick reference.
# MAGIC   * Google for **Spark API Latest**, select **Spark API Documentation - Spark** _x.x.x_ **Documentation - Apache Spark** and then select **Spark Python API (Sphinx)**.
# MAGIC   * <a href="https://spark.apache.org/docs/latest/api/python" target="_blank">Spark Python API (Sphinx) - Latest</a>
# MAGIC   * <a href="https://spark.apache.org/docs/2.1.0/api/python" target="_blank">Spark Python API (Sphinx) - 2.1.0</a>
# MAGIC   * <a href="https://spark.apache.org/docs/2.0.2/api/python" target="_blank">Spark Python API (Sphinx) - 2.0.2</a>
# MAGIC   * <a href="https://spark.apache.org/docs/1.6.3/api/python" target="_blank">Spark Python API (Sphinx) - 1.6.3</a>
# MAGIC 0. Look up the documentation for `pyspark.sql.DataFrame.show()`.
# MAGIC 0. Call `show()` on this `DataFrame` and set its parameter to display 50 lines

# COMMAND ----------

# TODO

# Type your answer here...

# COMMAND ----------

# MAGIC %md 
# MAGIC Besides looking up the documentation, Databrick's notebooks also provides [some] context support in the form of autocompletion.

# COMMAND ----------

# Place cursor after the period and hit the [Tab] key
spark.

# COMMAND ----------

# MAGIC %md **NOTE:** The notebook's ability to make suggestions is limited by the complexity of the expression(s) and the language (Scala vs Python)

# COMMAND ----------

# MAGIC %md Clearly we can count the number of records printed by the `show(..)` but of course it would be much easier to just use the `count()` action.

# COMMAND ----------

(pagecounts_en_all_df
  .select('project')
  .distinct()
  .count()
)

# COMMAND ----------

# MAGIC %md **Challenge 2: ** Can you figure out how to show [any] 10 articles that saw traffic during that hour?
# MAGIC 
# MAGIC **Bonus**: Show the 10 articles without truncating the name of the article.

# COMMAND ----------

# TODO

# Type your answer here...

# COMMAND ----------

# MAGIC %md ####![Wikipedia + Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/wiki_spark_small.png) **Q-2) How much traffic did each English Wikimedia project get during that hour?**

# COMMAND ----------

# MAGIC %md The following command will show the total number of requests each English Wikimedia project received.
# MAGIC 
# MAGIC To do this, we introduce the transformations `groupBy(..)`, `sum()` and `orderBy(..)`.

# COMMAND ----------

pagecounts_en_all_sum_df = (pagecounts_en_all_df.
    select('project', 'requests').               # transformation
    groupBy('project').                          # transformation
    sum()                                        # transformation
)

(pagecounts_en_all_sum_df.
    orderBy('sum(requests)').                    # transformation
    show()                                       # action
)

# COMMAND ----------

# MAGIC %md Our solution isn't quite right - we need to reverse the sort order.
# MAGIC 
# MAGIC Or to say that in SQL, we need to ORDER BY DESC.

# COMMAND ----------

pagecounts_en_all_sum_df_2 = (pagecounts_en_all_df.
    select('project', 'requests').               # transformation
    groupBy('project').                          # transformation
    sum()                                        # transformation
)

(pagecounts_en_all_sum_df_2.
    orderBy(  pagecounts_en_all_sum_df_2['sum(requests)'].desc()  ).  # transformation
    show()                                                            # action
)

# COMMAND ----------

# MAGIC %md English Wikipedia desktop (**en**) typically gets the highest number of requests, followed by English Wikimedia (**en.m**).

# COMMAND ----------

# MAGIC %md Notice anything unusual in that last command?
# MAGIC 
# MAGIC Let's take a look at the Scala Doc for the `Column` object. </br>
# MAGIC 
# MAGIC **REMINDER:** You can Google for **Spark API Latest**
# MAGIC 
# MAGIC *Note: Scala also has a couple subclasses (`ColumnName` and `TypedColumn`) not represented in Python.*

# COMMAND ----------

pagecounts_en_all_df.project

# COMMAND ----------

# MAGIC %md `Column` objects provide us a programatic way to build up SQL-ish expressions.
# MAGIC 
# MAGIC Here is a preview of the various functions:
# MAGIC 
# MAGIC | Column Functions |
# MAGIC | ---------------- |
# MAGIC | Alias |
# MAGIC | As |
# MAGIC | Asc (ascending) |
# MAGIC | As Type |
# MAGIC | Between |
# MAGIC | Bitwise AND |
# MAGIC | Bitwise OR |
# MAGIC | Bitwise XOR |
# MAGIC | Cast |
# MAGIC | Desc (descending) |
# MAGIC | Ends With |
# MAGIC | Is Not Null |
# MAGIC | Is Null |
# MAGIC | Is In |
# MAGIC | Like |
# MAGIC | Name |
# MAGIC | Otherwise |
# MAGIC | Over |
# MAGIC | R-Like |
# MAGIC | Starts With |
# MAGIC | Substr |
# MAGIC | When |
# MAGIC 
# MAGIC The complete list of functions differ from language to language.

# COMMAND ----------

# MAGIC %md 
# MAGIC In Python there is a slightly easier way to ORDER BY DESC:
# MAGIC 
# MAGIC `orderBy('sum(requests)', ascending=False)`

# COMMAND ----------

(pagecounts_en_all_df.
    select('project', 'requests').
    groupBy('project').
    sum().
    orderBy('sum(requests)', ascending=False).
    show()
)

# COMMAND ----------

# MAGIC %md ####![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_spark_tiny.png) **Transformations and Actions**

# COMMAND ----------

# MAGIC %md 
# MAGIC ![Spark Operations](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/spark_ta.png)

# COMMAND ----------

# MAGIC %md `DataFrames` support two basic types of operations: **Transformations** and **Actions**.
# MAGIC 
# MAGIC **Actions**, like `show()` or `count()`, return a value with results to the user. Other actions like `save()` write the DataFrame to distributed storage (like S3 or HDFS).
# MAGIC 
# MAGIC **Transformations**, like `select()` or `filter()` create a new `DataFrame` from an existing one.
# MAGIC   * They contribute to a query plan, but nothing is executed until an action is called.
# MAGIC   * **Typed Transformations** are part of the strongly-typed, `Dataset` API such as `Dataset[Customer]` or `Dataset[Vehicle]`
# MAGIC   * **Untyped Transformations** use the `DataFrames` API which is an alias for `Dataset[Row]`
# MAGIC 
# MAGIC | Typed Transformations  | Untyped Transformations | Actions            |
# MAGIC |:----------------------:|:-----------------------:|:------------------:|
# MAGIC | Alias                  | Agg (aggregate)         | Collect            |
# MAGIC | As                     | Apply                   | Count              |
# MAGIC | Coalesce               | Col (column)            | Describe           |
# MAGIC | Distinct               | Cross Join              | First              |
# MAGIC | Drop Duplicates        | Cube                    | For Each           |
# MAGIC | Except                 | Drop                    | For Each Partition |
# MAGIC | Filter                 | Group By                | Head               |
# MAGIC | Flat Map               | Join                    | Reduce             |
# MAGIC | Group By Key           | Rollup                  | Show               |
# MAGIC | Intersect              | Select                  | Take               |
# MAGIC | Join                   | Select Expr             |                    |
# MAGIC | Limit                  | Stat                    |                    |
# MAGIC | Map                    | With Column             |                    |
# MAGIC | Map Partitions         | With Column Renamed     |                    |
# MAGIC | Order By               |                         |                    |
# MAGIC | Random Split           |                         |                    |
# MAGIC | Repartition            |                         |                    |
# MAGIC | Sample                 |                         |                    |
# MAGIC | Select                 |                         |                    |
# MAGIC | Sort                   |                         |                    |
# MAGIC | Sort Within Partitions |                         |                    |
# MAGIC | Union                  |                         |                    |
# MAGIC | Where                  |                         |                    |

# COMMAND ----------

# MAGIC %md ####![Wikipedia + Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/wiki_spark_small.png) **Q-3) What were the 25 most popular English articles during the past hour?**

# COMMAND ----------

# MAGIC %md The `filter()` transformation can be used to filter a `DataFrame` where the language column is **en**, meaning English Wikipedia articles only:

# COMMAND ----------

# Only rows for for English Wikipedia (en) will pass this filter, removing projects like Wiktionary, Wikibooks, Wikinews, etc
pagecounts_en_wikipedia_df = (pagecounts_en_all_df
  .filter(pagecounts_en_all_df['project'] == "en")
)

# COMMAND ----------

# MAGIC %md Notice above that transformations, like `filter()`, return back a `DataFrame`.

# COMMAND ----------

# MAGIC %md Next, we can use the `orderBy()` transformation on the requests column to order the requests in descending order:

# COMMAND ----------

# Order by the requests column, in descending order
(pagecounts_en_wikipedia_df
   .orderBy('requests', ascending=False)  # transformation
   .show(25, False)                       # action
)

# COMMAND ----------

# MAGIC %md In Databricks, there is a special action that renders a `DataFrame` in an HTML table simmilarly to the `show(..)` action.
# MAGIC 
# MAGIC Unlike `show(..)`, the action `display(..)` does not allow us to specify the number of rows.
# MAGIC 
# MAGIC Instead we can use the transformation `limit(n)` which produces a new `DataFrame` containing only the first **n** rows.

# COMMAND ----------

most_popular_en_wikipedia_df = (pagecounts_en_wikipedia_df
  .orderBy('requests', ascending=False)
  .limit(25)
)

# Display the DataFrame as an HTML table so it's easier to read.
display(most_popular_en_wikipedia_df)

# COMMAND ----------

# MAGIC %md Hmm, the result doesn't look correct.
# MAGIC 
# MAGIC The article column contains a number of non-articles like:
# MAGIC * **Special:**
# MAGIC * **File:**
# MAGIC * **Category:**
# MAGIC * **Portal:**
# MAGIC * etc.
# MAGIC 
# MAGIC Let's take a look at Wikipedia Namespaces so we can filter the non-articles out...

# COMMAND ----------

# MAGIC %md ####![Wikipedia Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_wikipedia_tiny.png) **Wikipedia Namespaces**

# COMMAND ----------

# MAGIC %md Wikipedia has many namespaces.
# MAGIC 
# MAGIC The 5.1 million English articles are in the 0 namespace *(in bold below)*.
# MAGIC 
# MAGIC The other namespaces are for things like:
# MAGIC - Wikipedia User profiles (**User:** namespace 2)
# MAGIC - Files like images or videos (**File:** namespace 6)
# MAGIC - Draft articles not yet ready for publishing (**Draft:** namespace 118)
# MAGIC 
# MAGIC **Wikipedia Namespaces**
# MAGIC 
# MAGIC | ID   | Subject           | Talk                   | ID   |
# MAGIC |:-------------------------------------------------------- |
# MAGIC | 0    | **(Main/Article)** | Talk                   | 1    |
# MAGIC | 2    | User               | User talk              | 3    |
# MAGIC | 4    | Wikipedia          | Wikipedia talk         | 5    |
# MAGIC | 6    | File               | File talk              | 7    |
# MAGIC | 8    | MediaWiki          | MediaWiki talk         | 9    |
# MAGIC | 10   | Tempalte           | Template talk          | 11   |
# MAGIC | 12   | Help               | Help talk              | 13   |
# MAGIC | 14   | Category           | Category talk          | 15   |
# MAGIC | 100  | Portal             | Portal talk            | 101  |
# MAGIC | 108  | Book               | Book talk              | 109  |
# MAGIC | 118  | Draft              | Draft talk             | 119  |
# MAGIC | 446  | Education Program  | Education Program talk | 447  |
# MAGIC | 710  | TimedText          | TimedText talk         | 711  |
# MAGIC | 828  | Module             | Module talk            | 829  |
# MAGIC | 2300 | Gadget             | Gadget talk            | 2301 |
# MAGIC | 2302 | Gadget definition  | Gadget definition talk | 2303 |
# MAGIC | 2600 | Topic              |                        |      |
# MAGIC 
# MAGIC Source: <a href="https://en.wikipedia.org/wiki/Wikipedia:Namespace" target="_blank">Wikipedia:Namespace</a>

# COMMAND ----------

# MAGIC %md For example, here is the **User:** page for Jimmy (Jimbo) Wales, a co-founder of Wikipedia:
# MAGIC 
# MAGIC <a href="https://en.wikipedia.org/wiki/User:Jimbo_Wales" target="_blank">User:Jimbo_Wales</a>

# COMMAND ----------

# MAGIC %md Which is different from the normal article page for Jimmy Wales *(this is the encyclopedic one)*:
# MAGIC 
# MAGIC <a href="https://en.wikipedia.org/wiki/Jimmy_Wales" target="_blank">Jimmy_Wales<a/>

# COMMAND ----------

# MAGIC %md Next, here is an image from the **File:** namespace of Jimmy Wales in 2010:
# MAGIC 
# MAGIC <a href="https://en.wikipedia.org/wiki/File:Jimmy_Wales_July_2010.jpg" target="_blank">File:Jimmy_Wales_July_2010.jpg</a>

# COMMAND ----------

# MAGIC %md The hourly pagecounts file contains traffic requests to all Wikipedia namespaces.
# MAGIC 
# MAGIC We'll need to filter out anything that is not an article.
# MAGIC 
# MAGIC Let's filter out everything that is not an article:

# COMMAND ----------

# The 17 filters will remove everything that is not an article

pagecounts_en_wikipedia_articles_only_df = (pagecounts_en_wikipedia_df
  .filter(pagecounts_en_wikipedia_df["article"].rlike(r'^((?!Special:)+)'))
  .filter(pagecounts_en_wikipedia_df["article"].rlike(r'^((?!File:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!Category:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!User:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!Talk:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!Template:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!Help:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!Wikipedia:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!MediaWiki:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!Portal:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!Book:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!Draft:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!Education_Program:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!TimedText:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!Module:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!Topic:)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!Images/)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!%22//upload.wikimedia.org)+)'))
  .filter(pagecounts_en_wikipedia_df['article'].rlike(r'^((?!%22//en.wikipedia.org)+)'))
)

pagecounts_en_wikipedia_articles_only_df.count()

# COMMAND ----------

# MAGIC %md The function `Column.rlike(..)` applies a regular expression to the specified column so as to determine if the record should be included or not.
# MAGIC 
# MAGIC However, regular expression have the problem of adding additional overhead in addition to precluding certain types of optimizations.
# MAGIC 
# MAGIC One alternative is the function `Column.startsWith(..)` as we see in this example

# COMMAND ----------

# The 17 filters using startsWith

pagecounts_en_wikipedia_articles_only_df_2 = (pagecounts_en_wikipedia_df
  .filter(pagecounts_en_wikipedia_df["article"].startswith('Special:') == False)
  .filter(pagecounts_en_wikipedia_df["article"].startswith('File:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('Category:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('User:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('Talk:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('Template:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('Help:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('Wikipedia:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('MediaWiki:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('Portal:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('Book:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('Draft:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('Education_Program:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('TimedText:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('Module:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('Topic:') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('Images/') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('%22//upload.wikimedia.org') == False)
  .filter(pagecounts_en_wikipedia_df['article'].startswith('%22//en.wikipedia.org') == False)
)

pagecounts_en_wikipedia_articles_only_df_2.count()

# COMMAND ----------

# MAGIC %md **Challenge 3:**
# MAGIC 
# MAGIC One more alternative is the function `Column.like(..)`.
# MAGIC 
# MAGIC Rework the previous example to use the SQL-equivilent `like(..)` command.

# COMMAND ----------

# TODO

pagecounts_en_wikipedia_articles_only_df_3 = (pagecounts_en_wikipedia_df
  # FILL-IN
)

total = pagecounts_en_wikipedia_articles_only_df_3.count()
assert total == 1263540, "Expected 1263540, found " + str(total)

# COMMAND ----------

# MAGIC %md Finally, re-apply the `orderBy(..)` and `limit(..)` transformations from earlier to show the 25 most requested pages.

# COMMAND ----------

display(
  pagecounts_en_wikipedia_articles_only_df.
    orderBy('requests', ascending=False).
    limit(25)
)

# COMMAND ----------

# MAGIC %md That looks better. Above you are seeing the 25 most requested English Wikipedia articles in the past hour!
# MAGIC 
# MAGIC This can give you a sense of what's popular or trending on the planet right now.

# COMMAND ----------

# MAGIC %md ####![Wikipedia + Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/wiki_spark_small.png) **Q-4) How many requests did the "Apache Spark" article receive during this hour? **

# COMMAND ----------

# MAGIC %md **Challenge 4: ** Can you figure out how to filter the `pagecountsEnWikipediaArticlesOnlyDF` DataFrame for just `Apache_Spark`?

# COMMAND ----------

# TODO
# Type your answer here.

# COMMAND ----------

# MAGIC %md ####![Wikipedia + Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/wiki_spark_small.png) **Q-5) Which Apache project received the most requests during this hour? **

# COMMAND ----------

# MAGIC %md **Challenge 5**: Complete the cell below to show only the Apache articles with the most requested articles listed first.
# MAGIC 
# MAGIC That is, those records where the column **article** begins with **Apache_** and ordered by **requests** descending.
# MAGIC 
# MAGIC We've seen at least three different functions that can solve this.
# MAGIC 
# MAGIC To review those options, look up the documentation for the `Column` object.

# COMMAND ----------

# In the Regular Expression below:
# ^  - Matches beginning of line
# .* - Matches any characters, except newline

(pagecounts_en_wikipedia_articles_only_df.
  # filter( FILL-IN ).
  # orderBy( FILL-IN ).
  show(20, False)
)

# COMMAND ----------

# MAGIC %md Rendering a nice ASCII table with the `show(..)` action solves our problem.
# MAGIC 
# MAGIC And of course we can also use the `display(..)` command to get a pretty result, even export the list as CSV.
# MAGIC 
# MAGIC But what if we want a list of those articles so that we can operate on 'em?
# MAGIC 
# MAGIC We can do this with the transformation `collect()` which retuns a collection to the driver for additional processing

# COMMAND ----------

articles = (pagecounts_en_wikipedia_articles_only_df.
  filter(pagecounts_en_wikipedia_articles_only_df["article"].startswith("Apache_")).
  orderBy('requests', ascending=False).
  collect()
)

for article in articles:
    print(">> " + str(article.asDict()))

# COMMAND ----------

# MAGIC %md 
# MAGIC Simmilar to the `collect()` action, we also have...
# MAGIC * `take(n)` which brings the first **n** records to the driver.
# MAGIC * `head()` or it's alias `first()` which brings the first (1) record to the driver.

# COMMAND ----------

# MAGIC %md ####![Wikipedia + Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/wiki_spark_small.png) **Q-6) What percentage of the 5.1 million English articles were requested during the hour?**

# COMMAND ----------

# MAGIC %md Start with the `DataFrame` that has already been filtered and contains just the English Wikipedia Articles (**pagecountsEnWikipediaArticlesOnlyDF**):

# COMMAND ----------

display(pagecounts_en_wikipedia_articles_only_df)

# COMMAND ----------

# MAGIC %md Call the `count()` action on the `DataFrame` to see how many unique English articles were requested in the last hour:

# COMMAND ----------

pagecounts_en_wikipedia_articles_only_df.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC The `count()` action returns back a `Long` data type in Scala and an `int` in Python.

# COMMAND ----------

# MAGIC %md There are currently about 5.1 million articles in English Wikipedia. So the percentage of English articles requested in the past hour is:

# COMMAND ----------

pagecounts_en_wikipedia_articles_only_df.count() / 5100000.0 * 100

# COMMAND ----------

# MAGIC %md ####![Wikipedia + Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/wiki_spark_small.png) **Q-7) How many total requests were there to English Wikipedia Desktop edition?**

# COMMAND ----------

# MAGIC %md The `DataFrame` holding English Wikipedia article requests has a 3rd column named `requests`:

# COMMAND ----------

pagecounts_en_wikipedia_articles_only_df.printSchema()

# COMMAND ----------

# MAGIC %md By using the `groupBy(..)` transformation on the **project** column and then calling `sum()`, we can count how many total requests there were to English Wikipedia:

# COMMAND ----------

display(
  pagecounts_en_wikipedia_df.
    groupBy("project").
    sum()
)

# COMMAND ----------

# MAGIC %md 
# MAGIC Again, printing an ASCII table with the `show(..)` action or in this case, the `display()` action, is fine if we only want to see the result, but what if we need the actual value for further processing?
# MAGIC 
# MAGIC Let's take a look at what the result of the previous operation is:

# COMMAND ----------

pagecounts_en_wikipedia_df_sum = (pagecounts_en_wikipedia_df.
    groupBy("project").
    sum()
)

pagecounts_en_wikipedia_df_sum # Included here only to force printing to the console below

# COMMAND ----------

# MAGIC %md 
# MAGIC **pagecounts_en_wikipedia_df_sum** is a `DataFrame` which is an alias for [Scala's] `Dataset[Row]`
# MAGIC 
# MAGIC This means that each record in the `DataFrame` is backed by a `Row` object.
# MAGIC 
# MAGIC We can use the `first()` or `head()` action to get the first (and only) record from our `DataFrame`

# COMMAND ----------

row = pagecounts_en_wikipedia_df_sum.first()
row # Included here only to force printing to the console below

# COMMAND ----------

# MAGIC %md Let's take a look at the documentation for the `Row` object so that we can see which operations are available to us.

# COMMAND ----------

desktop_total = row['sum(requests)']
print("Desktop Total: ", desktop_total )

# COMMAND ----------

# MAGIC %md ####![Wikipedia + Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/wiki_spark_small.png) **Q-8) How many total requests were there to English Wikimedia edition?**

# COMMAND ----------

# MAGIC %md We'll need to start with the original `DataFrame` which contains all the English Wikimedia project requests:

# COMMAND ----------

display(
  pagecounts_en_all_df.
    limit(5)
)

# COMMAND ----------

# MAGIC %md **Challenge 6**: Set the table for answering this business question:
# MAGIC 1. First run a `filter()` to keep just the rows referring to the <a href="https://www.wikimedia.org/" target="_blank">English Wikimedia</a> project (**project** equals **en.m**).
# MAGIC 2. Count the rows in the resulting `DataFrame`.

# COMMAND ----------

# TODO

# pagecounts_en_wm_df = pagecounts_en_all_df.<<your filter expression comes here>>

# pagecounts_en_wm_df.<<the action for counting elements comes here>>

# COMMAND ----------

# MAGIC %md Okay, what do we have?

# COMMAND ----------

display(pagecounts_en_wm_df)

# COMMAND ----------

# MAGIC %md Let's aggregate the results.
# MAGIC 
# MAGIC Last time we used the transformation `groupBy(..)` and `sum()`.
# MAGIC 
# MAGIC Let's try an alternate approach.
# MAGIC 
# MAGIC First, we will need to import the SQL functions package, which includes statistical functions like sum(), max(), min(), avg(), etc.

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md 
# MAGIC Let's take a look at the documentation for the SQL functions:
# MAGIC 
# MAGIC If you don't have the documentation still open
# MAGIC * Goolge for **Spark API Latest**
# MAGIC * Select the link for **Spark API Documentation - Spark** _x.x.x_ **Documentation - Apache Spark**
# MAGIC * Select the **Spark Python API (Sphinx)**
# MAGIC 
# MAGIC With the documentation open
# MAGIC * Search for **functions**.
# MAGIC * Select the **pyspark.sql.functions** link.
# MAGIC * Look for the `sum(..)` function.
# MAGIC * Review the other functions

# COMMAND ----------

media_total = (pagecounts_en_wm_df.
  select( sum(pagecounts_en_wm_df['requests']) ).
  first()
  ['sum(requests)']
)

media_total

# COMMAND ----------

print("Media Total: " + str(media_total) )
print("Desktop Total: " + str(desktop_total) )
print("Ratio: " + str(100.0*media_total/desktop_total) )

# COMMAND ----------

# MAGIC %md Our results indicate that the <a href="https://www.wikipedia.org" target="_blank">English Wikipedia</a> articles get roughly 66.7% more requests than the <a href="https://www.wikimedia.org" target="_blank">English Wikimedia</a> articles.