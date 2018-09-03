import org.apache.spark.sql.SparkSession
import org.structured_streaming_sources.twitter.TwitterStreamingSource
import org.apache.spark.sql.types.{DataType, TimestampType}
//import org.apache.spark.sqlContext.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
//import org.apache.spark.implicits._
import org.apache.spark.sql.streaming._

/**
  * Created by hluu on 3/11/18.
  */
object TwitterSourceExample {
  private val SOURCE_PROVIDER_CLASS = TwitterStreamingSource.getClass.getCanonicalName

  def main(args: Array[String]): Unit = {
    println("TwitterSourceExample")

    val providerClassName = SOURCE_PROVIDER_CLASS.substring(0, SOURCE_PROVIDER_CLASS.indexOf("$"))
    println(providerClassName)

    if (args.length != 4) {
      println("Usage: <consumer key>, <consumer secret> <access token> <access token secret>")
      sys.exit(1)
    }


    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    // create a Spark session
    val spark = SparkSession
      .builder
      .appName("TwitterStructuredStreaming")
      .master("local[*]")
      .getOrCreate()
      
    import spark.implicits._

    println("Spark version: " + spark.version)

    val stopWordsRDD = spark.sparkContext.textFile("src/main/resources/stop-words.txt")
    val posWordsRDD = spark.sparkContext.textFile("src/main/resources/pos-words.txt")
    val negWordsRDD = spark.sparkContext.textFile("src/main/resources/neg-words.txt")

    val positiveWords = posWordsRDD.collect().toSet
    val negativeWords = negWordsRDD.collect().toSet
    val stopWords = stopWordsRDD.collect().toSet

    val parquetOutputPath = "target/output5/"
    val checkpointPath = "target/output5.checkpoint/"

    
    val tweetSchema = new StructType()
                          .add("text",StringType)
                          .add("user",StringType)
                          .add("userLang",StringType)
                          .add("createdDate", TimestampType)
                          .add("isRetweeted", BooleanType)

    
    val tweetDF = spark.readStream
                       .format(providerClassName)
                       //.schema(tweetSchema)
                       //.option("maxFilesPerTrigger", "100")
                       .option("inferSchema","true")
                       .option(TwitterStreamingSource.CONSUMER_KEY, consumerKey)
                       .option(TwitterStreamingSource.CONSUMER_SECRET, consumerSecret)
                       .option(TwitterStreamingSource.ACCESS_TOKEN, accessToken)
                       .option(TwitterStreamingSource.ACCESS_TOKEN_SECRET, accessTokenSecret)
                         .load()

    tweetDF.printSchema()
   
    /*
    // See an example of what the data looks like
    // by printing out a Row
    val colnames = tweetDF.columns
    val firstrow = tweetDF.head(1)(0)
    println("\n")
    println("Example Data Row")
    for(ind <- Range(1,colnames.length)){
      println(colnames(ind))
      println(firstrow(ind))
      println("\n")
    }
    * 
    */
    
    //take 1
    //val tweetDFWithTS = tweetDF.withColumn("process_time", current_timestamp())
    //tweetDFWithTS.printSchema
    
    //val enTweetsDF = tweetDF.filter(userLang => userLang = "en")
    //val enTweetsDF2 = tweetDF.filter($"userLang" == "en")
    //en-gb
    //val enTweetsDF3 = tweetDF.filter(status => status.getUser().getLang() == "en")
    
    //val groupByWindow = tweetDF.groupBy(window($"createdDate", "10 seconds", "5 seconds"))
                                     //.agg(sum("request_count").as("total_count"))
    
    
    //take 2
    //next do contains en
    /*
    val tweetsEnDF = tweetDF
       .select(explode($"root") as "tweet")
       .select(
        "tweet.text"
        , "tweet.user"
        , "tweet.userLang"
        , unix_timestamp($"tweet.createdDate", "yyyy-MM-dd'T'hh:mm:ss").cast("timestamp") as "timestamp"
         , "isRetweeted")
                 //"createdDate", "isRetweeted")
                  *
                  */
    //.where($"userLang" === "en") 
    
    val tweetEnDF = tweetDF.select("text", "user", "userLang", "createdDate", "isRetweeted").where($"userLang" === "en")
    
    //val tweetEnWithTSDF = tweetEnDF.withColumn("process_time", current_timestamp())
    
    //val tweetEnOutDF = tweetEnWithTSDF.groupBy(window($"process_time", "10 seconds", "5 seconds"))
    
    
    //val tweetsEnWordsDF = tweetsEnDF.select("text").flatMap(t => t.toLowerCase.split(" "))
    //as[String].flatMap(_.split(" "))
    
    
    //val tweetsEnWordsCountDF = tweetsEnWordsDF.groupBy($"action", window($"createdDate", "10 seconds")).count()
    
    //tweetsEnWordsCountDF.writeStream.format(“parquet”).option(”path”,“<path>”)              
    //writeStream.format(“parquet”).start()
    // Console sink
    
    //val tweetQS2 = tweetsEnWordsDF.writeStream.format("console").option("truncate", false).start()
    //val tweetQS = tweetDF.writeStream.format("console").option("truncate", false).start()


    //take 3
    //val countDF = tweetDF.groupBy(window($"createdDate", "10 seconds", "30 seconds"), $"text").count()
    //countDF.writeStream.format("memory").queryName("tweet") .outputMode("complete").start()
    //sql("select sum(count) from tweet where userLang='en'")
    
    tweetEnDF.writeStream
           .trigger(ProcessingTime("10 seconds"))
           .format("parquet")
           //.queryName("tweets")
           //.outputMode("append")
           .partitionBy("userLang")
           .option("path", parquetOutputPath)
           .option("checkpointLocation", checkpointPath)
           .start()              

           
    //tweetDF.show()
        
    Thread.sleep(1000 * 35)
     
    //tweetQS2.awaitTermination()
  }
}
