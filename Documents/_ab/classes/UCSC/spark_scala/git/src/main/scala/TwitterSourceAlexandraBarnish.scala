import org.apache.spark.sql.SparkSession
import org.structured_streaming_sources.twitter.TwitterStreamingSource
import org.apache.spark.sql.types.{DataType, TimestampType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


/**
  * Created by abarnish on 9/2/18
  */
object TwitterSourceAlexandraBarnish {
  private val SOURCE_PROVIDER_CLASS = TwitterStreamingSource.getClass.getCanonicalName

  def main(args: Array[String]): Unit = {
    println("TwitterSourceAlexandraBarnish")

    val providerClassName = SOURCE_PROVIDER_CLASS.substring(0, SOURCE_PROVIDER_CLASS.indexOf("$"))
    println(providerClassName)

    if (args.length != 4) {
      println("Usage: <consumer key>, <consumer secret> <access token> <access token secret>")
      sys.exit(1)
    }

    //authentication
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    // create a Spark session
    val spark = SparkSession
      .builder
      .appName("TwitterStructuredStreaming")
      .master("local[*]")
      .getOrCreate()

    println("Spark version: " + spark.version)

    //set up static comparisons
    val stopWordsRDD = spark.sparkContext.textFile("src/main/resources/stop-words.txt")
    val posWordsRDD = spark.sparkContext.textFile("src/main/resources/pos-words.txt")
    val negWordsRDD = spark.sparkContext.textFile("src/main/resources/neg-words.txt")

    val positiveWordsSet = posWordsRDD.collect().toSet
    val negativeWordsSet = negWordsRDD.collect().toSet
    val stopWordsSet = stopWordsRDD.collect().toSet

    //set up schema
    val twitterSchema = new StructType()
       .add("text", StringType, true)
       .add("user", StringType, true)
       .add("userLang", StringType, true)
       .add("createdDate", TimestampType, true)
       .add("isRetweeted", StringType, true)
     
    //set up streaming 
    val tweetDF = spark.readStream
                       .format(providerClassName)
                       .schema(twitterSchema)
                       .option(TwitterStreamingSource.CONSUMER_KEY, consumerKey)
                       .option(TwitterStreamingSource.CONSUMER_SECRET, consumerSecret)
                       .option(TwitterStreamingSource.ACCESS_TOKEN, accessToken)
                       .option(TwitterStreamingSource.ACCESS_TOKEN_SECRET, accessTokenSecret)
                         .load()

    tweetDF.printSchema()
    import spark.implicits._
    
    // select tweets where lang contains en, split into lower case words
    val tweetStreamEnDF = tweetStream.withColumn("text", explode(split(lower($"text"), "[ ]")))
        .select($"text", $"createdDate").where($"userLang".contains("en") && $"text".isNotNull)
    
    // aggregate over a sliding event time window, 10 seconds or 30 seconds
    val tweetEnGroupByWindow = tweetStreamEnDF
        .groupBy(window($"createdDate", "10 seconds", "2 seconds"), $"text").count()

    //remove punctuation from tweets
    val tweetStreamEnWordsNoPuncDF = tweetEnGroupByWindow
        .withColumn("text", regexp_replace(tweetEnGroupByWindow("text"), "[\',.!?@#\\>:;=\"<$%^&*~`|\\{\\}\\[\\]\\(\\)]", ""))
        .select($"text")

    // write output early to be able to convert to set??
    val groupByWindowTweetStreamQuery = tweetStreamEnWordsNoPuncDF
                             .writeStream
                             .format("memory")
                             .outputMode("complete")
                             .queryName("tweets")
                             .start()
                             
    // collect all words into a set
    val tweetStreamEnWordsSet = spark.sql("select * from tweets").map(_.getString(0)).collect().toSet
    
    // subtract stop words
    val tweetStreamEnWordsNoPuncNoStopsSet = tweetStreamEnWordsSet -- stopWordsSet
    
    // pull out positive words
    val tweetStreamEnPositive = tweetStreamEnWordsNoPuncNoStopsSet.intersect(positiveWordsSet)
    
    // add +1 for each positive word
    val tweetStreamEnPositiveSet = tweetStreamEnPositive.map(t => (t, 1))
    
    // sub-total positive words
    val positiveTotal = tweetStreamEnPositiveSet.toList.map(i => i._2).sum
    
    // pull out negative words
    val tweetStreamEnNegative = tweetStreamEnWordsNoPuncNoStopsSet.intersect(negativeWordsSet)
    
    // add -1 for each negative word
    val tweetStreamEnNegativeSet = tweetStreamEnNegative.map(t => (t, -1))
    
    // sub-total negative words
    val negativeTotal = tweetStreamEnNegativeSet.toList.map(i => i._2).sum
    
    // total positive & negative
    val totalCount = negativeTotal + positiveTotal
    
    // UDF translate total to sentiment 
    def tweetSentiment(totalCount:Integer) : String = { 
        totalCount match {
            case totalCount if totalCount == 0 => "neutral"
            case totalCount if totalCount >= 1 => "positive"
            case totalCount if totalCount <= -1 => "negative"
        }
     }
    
    import org.apache.spark.sql.functions._ 
    
    // register a scala function for use on DF
    val tweetSentimentUDF = udf(tweetSentiment(_:Integer)) 
    
    // load total to DF
    val totalCountDF = sc.parallelize(Seq(totalCount)).toDF
    
    // use UDF to determine sentiment
    val tweetSentimentDF = totalCountDF.select(tweetSentimentUDF($"value"))
        .withColumnRenamed("UDF(value)", "sentiment").groupBy($"sentiment").count()
    
    // display
    tweetSentimentDF.show
    
    // not used
    //val tweetQS = tweetDF.writeStream.format("console").option("truncate", false).start()

    Thread.sleep(1000 * 35)

    groupByWindowTweetStreamQuery.awaitTermination()
  }
}
