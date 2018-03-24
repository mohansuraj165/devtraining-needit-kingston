package apps1
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

case class Customer(cId: Long, name : String, age: Int, gender : String)
case class Customer2(custId: Long, name : String, age: Int, gender : String)
case class Product(pId: Int, product: String, price : Double, cost: Double)
case class Transaction(tId : Long, custId : Long, prodId : Long, date : String, city : String)


object Asgn7 {
  Logger.getLogger("org").setLevel(Level.OFF)
 	val spark=SparkSession.builder().master("local[*]").appName("IFT443Fall2017").getOrCreate()
                                                  //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| spark  : org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSessi
                                                  //| on@7cb8437d
 import spark.implicits._
 spark.version                                    //> res0: String = 2.2.0
 val sc = spark.sparkContext                      //> sc  : org.apache.spark.SparkContext = org.apache.spark.SparkContext@fca387
 //create 2 dataframes with customer data
 val customerDF = List(Customer(1,"A", 18,  "M"),
 												Customer(2,"B", 21,  "F"),
 												Customer(11, "Jackson", 21, "M"),
												Customer(12, "Emma", 25, "F"),
												Customer(13, "Olivia", 31, "F"),
 												Customer(6,"F", 71, "F"),
 												Customer(6,"F", 71, "F")).toDF()
                                                  //> customerDF  : org.apache.spark.sql.DataFrame = [cId: bigint, name: string .
                                                  //| .. 2 more fields]
 												
 val customer2DF = List(Customer2(11, "Jackson", 21, "M"),
Customer2(12, "Emma", 25, "F"),
Customer2(13, "Olivia", 31, "F"),
Customer2(4, "Jennifer", 45, "F"),
Customer2(5, "Robert", 41, "M"),
Customer2(6, "Sandra", 45, "F")).toDF()           //> customer2DF  : org.apache.spark.sql.DataFrame = [custId: bigint, name: stri
                                                  //| ng ... 2 more fields]
//customerDF.show()
 //===Basic Operations===
 
customerDF.cache()                                //> res1: apps1.Asgn7.customerDF.type = [cId: bigint, name: string ... 2 more f
                                                  //| ields]
spark.sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed","true")

val cols = customerDF.columns                     //> cols  : Array[String] = Array(cId, name, age, gender)
val columnsWithTypes = customerDF.dtypes          //> columnsWithTypes  : Array[(String, String)] = Array((cId,LongType), (name,S
                                                  //| tringType), (age,IntegerType), (gender,StringType))
customerDF.explain()                              //> == Physical Plan ==
                                                  //| InMemoryTableScan [cId#4L, name#5, age#6, gender#7]
                                                  //|    +- InMemoryRelation [cId#4L, name#5, age#6, gender#7], true, 10000, Stor
                                                  //| ageLevel(disk, memory, deserialized, 1 replicas)
                                                  //|          +- LocalTableScan [cId#4L, name#5, age#6, gender#7]
customerDF.persist                                //> res2: apps1.Asgn7.customerDF.type = [cId: bigint, name: string ... 2 more f
                                                  //| ields]
customerDF.printSchema()                          //> root
                                                  //|  |-- cId: long (nullable = false)
                                                  //|  |-- name: string (nullable = true)
                                                  //|  |-- age: integer (nullable = false)
                                                  //|  |-- gender: string (nullable = true)
                                                  //| 
customerDF.registerTempTable("customer")

val resultDF =spark.sql("Select count(1) From customer")
                                                  //> resultDF  : org.apache.spark.sql.DataFrame = [count(1): bigint]
val countDFX = resultDF.toDF("MyCount")           //> countDFX  : org.apache.spark.sql.DataFrame = [MyCount: bigint]

//===Language Integrated query===

//agg
val aggregatesCust = customerDF.agg(max("age"), min("age"))
                                                  //> aggregatesCust  : org.apache.spark.sql.DataFrame = [max(age): int, min(age)
                                                  //| : int]
 aggregatesCust.show()                            //> [Stage 0:>                                                          (0 + 0
                                                  //| ) / 4]                                                                    
                                                  //|             +--------+--------+
                                                  //| |max(age)|min(age)|
                                                  //| +--------+--------+
                                                  //| |      71|      18|
                                                  //| +--------+--------+
                                                  //| 
//apply
//val priceColumn = productDF.apply("price")

//distinct
val distinct = customerDF.distinct                //> distinct  : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [cId: 
                                                  //| bigint, name: string ... 2 more fields]
distinct.show()                                   //> [Stage 9:=====================================================>  (96 + 4) 
                                                  //| / 100]                                                                    
                                                  //|             +---+-------+---+------+
                                                  //| |cId|   name|age|gender|
                                                  //| +---+-------+---+------+
                                                  //| | 12|   Emma| 25|     F|
                                                  //| |  2|      B| 21|     F|
                                                  //| |  6|      F| 71|     F|
                                                  //| | 13| Olivia| 31|     F|
                                                  //| | 11|Jackson| 21|     M|
                                                  //| |  1|      A| 18|     M|
                                                  //| +---+-------+---+------+
                                                  //| 

//filter
val filteredCustomer = customerDF.filter("age > 25")
                                                  //> filteredCustomer  : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] 
                                                  //| = [cId: bigint, name: string ... 2 more fields]
filteredCustomer.show()                           //> +---+------+---+------+
                                                  //| |cId|  name|age|gender|
                                                  //| +---+------+---+------+
                                                  //| | 13|Olivia| 31|     F|
                                                  //| |  6|     F| 71|     F|
                                                  //| |  6|     F| 71|     F|
                                                  //| +---+------+---+------+
                                                  //| 
 //groupBy
 val genderCount = customerDF .groupBy("gender").count
                                                  //> genderCount  : org.apache.spark.sql.DataFrame = [gender: string, count: big
                                                  //| int]
 genderCount.show()                               //> +------+-----+
                                                  //| |gender|count|
                                                  //| +------+-----+
                                                  //| |     F|    5|
                                                  //| |     M|    2|
                                                  //| +------+-----+
                                                  //| 

//intersect
val intersect = customerDF.intersect(customer2DF) //> intersect  : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [cId:
                                                  //|  bigint, name: string ... 2 more fields]
intersect.show()                                  //> +---+-------+---+------+
                                                  //| |cId|   name|age|gender|
                                                  //| +---+-------+---+------+
                                                  //| | 12|   Emma| 25|     F|
                                                  //| | 13| Olivia| 31|     F|
                                                  //| | 11|Jackson| 21|     M|
                                                  //| +---+-------+---+------+
                                                  //| 
 
 //join
 val join = customerDF.join(customer2DF, $"cId" === $"custId", "inner")
                                                  //> join  : org.apache.spark.sql.DataFrame = [cId: bigint, name: string ... 6 m
                                                  //| ore fields]
 join.show()                                      //> +---+-------+---+------+------+-------+---+------+
                                                  //| |cId|   name|age|gender|custId|   name|age|gender|
                                                  //| +---+-------+---+------+------+-------+---+------+
                                                  //| | 11|Jackson| 21|     M|    11|Jackson| 21|     M|
                                                  //| | 12|   Emma| 25|     F|    12|   Emma| 25|     F|
                                                  //| | 13| Olivia| 31|     F|    13| Olivia| 31|     F|
                                                  //| |  6|      F| 71|     F|     6| Sandra| 45|     F|
                                                  //| |  6|      F| 71|     F|     6| Sandra| 45|     F|
                                                  //| +---+-------+---+------+------+-------+---+------+
                                                  //| 
 
//limit
val limitFive = customerDF.limit(5)               //> limitFive  : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [cId:
                                                  //|  bigint, name: string ... 2 more fields]
limitFive.show()                                  //> +---+-------+---+------+
                                                  //| |cId|   name|age|gender|
                                                  //| +---+-------+---+------+
                                                  //| |  1|      A| 18|     M|
                                                  //| |  2|      B| 21|     F|
                                                  //| | 11|Jackson| 21|     M|
                                                  //| | 12|   Emma| 25|     F|
                                                  //| | 13| Olivia| 31|     F|
                                                  //| +---+-------+---+------+
                                                  //| 

//orderBy
val sort = customerDF.orderBy("age")              //> sort  : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [cId: bigi
                                                  //| nt, name: string ... 2 more fields]
sort.show()                                       //> +---+-------+---+------+
                                                  //| |cId|   name|age|gender|
                                                  //| +---+-------+---+------+
                                                  //| |  1|      A| 18|     M|
                                                  //| | 11|Jackson| 21|     M|
                                                  //| |  2|      B| 21|     F|
                                                  //| | 12|   Emma| 25|     F|
                                                  //| | 13| Olivia| 31|     F|
                                                  //| |  6|      F| 71|     F|
                                                  //| |  6|      F| 71|     F|
                                                  //| +---+-------+---+------+
                                                  //| 
                                                  
//randomSplit
val dfArray = customerDF.randomSplit(Array(0.6, 0.4))
                                                  //> dfArray  : Array[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]] = 
                                                  //| Array([cId: bigint, name: string ... 2 more fields], [cId: bigint, name: st
                                                  //| ring ... 2 more fields])
dfArray(0).count                                  //> res3: Long = 4
dfArray(1).count                                  //> res4: Long = 3

//rollUp
val rollup = customer2DF.rollup($"gender").avg("age")
                                                  //> rollup  : org.apache.spark.sql.DataFrame = [gender: string, avg(age): doubl
                                                  //| e]
rollup.show()                                     //> +------+------------------+
                                                  //| |gender|          avg(age)|
                                                  //| +------+------------------+
                                                  //| |  null|34.666666666666664|
                                                  //| |     F|              36.5|
                                                  //| |     M|              31.0|
                                                  //| +------+------------------+
                                                  //| 
                                                  
val sample = customerDF.sample(true, 0.5)         //> sample  : org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [cId: bi
                                                  //| gint, name: string ... 2 more fields]
sample.show()                                     //> +---+----+---+------+
                                                  //| |cId|name|age|gender|
                                                  //| +---+----+---+------+
                                                  //| |  1|   A| 18|     M|
                                                  //| |  2|   B| 21|     F|
                                                  //| | 12|Emma| 25|     F|
                                                  //| |  6|   F| 71|     F|
                                                  //| +---+----+---+------+
                                                  //| 
                                                  
//select
val namesAge = customer2DF.select("name", "age")  //> namesAge  : org.apache.spark.sql.DataFrame = [name: string, age: int]
namesAge.show()                                   //> +--------+---+
                                                  //| |    name|age|
                                                  //| +--------+---+
                                                  //| | Jackson| 21|
                                                  //| |    Emma| 25|
                                                  //| |  Olivia| 31|
                                                  //| |Jennifer| 45|
                                                  //| |  Robert| 41|
                                                  //| |  Sandra| 45|
                                                  //| +--------+---+
                                                  //| 

//selectExpr
val selectExpr = customer2DF.selectExpr("custId + 100 AS new_age", "name")
                                                  //> selectExpr  : org.apache.spark.sql.DataFrame = [new_age: bigint, name: stri
                                                  //| ng]
selectExpr.show()                                 //> +-------+--------+
                                                  //| |new_age|    name|
                                                  //| +-------+--------+
                                                  //| |    111| Jackson|
                                                  //| |    112|    Emma|
                                                  //| |    113|  Olivia|
                                                  //| |    104|Jennifer|
                                                  //| |    105|  Robert|
                                                  //| |    106|  Sandra|
                                                  //| +-------+--------+
                                                  //| 
 
 
 //===RDD operations===
 val rdd = customer2DF.rdd                        //> rdd  : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRD
                                                  //| D[63] at rdd at apps1.Asgn7.scala:112
 val firstRow = rdd.first                         //> firstRow  : org.apache.spark.sql.Row = [11,Jackson,21,M]
 val name = firstRow.getString(1)                 //> name  : String = Jackson
 val age = firstRow.getInt(2)                     //> age  : Int = 21
 
 //toJSON
 val jsonRDD = customer2DF.toJSON                 //> jsonRDD  : org.apache.spark.sql.Dataset[String] = [value: string]
 jsonRDD.collect()foreach(println)                //> {"custId":11,"name":"Jackson","age":21,"gender":"M"}
                                                  //| {"custId":12,"name":"Emma","age":25,"gender":"F"}
                                                  //| {"custId":13,"name":"Olivia","age":31,"gender":"F"}
                                                  //| {"custId":4,"name":"Jennifer","age":45,"gender":"F"}
                                                  //| {"custId":5,"name":"Robert","age":41,"gender":"M"}
                                                  //| {"custId":6,"name":"Sandra","age":45,"gender":"F"}
 
 //===Actions===
 
 //collect
 val result = customerDF.collect                  //> result  : Array[org.apache.spark.sql.Row] = Array([1,A,18,M], [2,B,21,F], [
                                                  //| 11,Jackson,21,M], [12,Emma,25,F], [13,Olivia,31,F], [6,F,71,F], [6,F,71,F])
                                                  //| 
 //count
 val count = customerDF.count                     //> count  : Long = 7
 
 //describe
 val summaryStatsDF = customer2DF.describe("age") //> summaryStatsDF  : org.apache.spark.sql.DataFrame = [summary: string, age: s
                                                  //| tring]
 summaryStatsDF.show                              //> +-------+------------------+
                                                  //| |summary|               age|
                                                  //| +-------+------------------+
                                                  //| |  count|                 6|
                                                  //| |   mean|34.666666666666664|
                                                  //| | stddev|10.462631918722298|
                                                  //| |    min|                21|
                                                  //| |    max|                45|
                                                  //| +-------+------------------+
                                                  //| 
 
 //first
 val first = customerDF.first                     //> first  : org.apache.spark.sql.Row = [1,A,18,M]
 
 //show
 customer2DF.show(2)                              //> +------+-------+---+------+
                                                  //| |custId|   name|age|gender|
                                                  //| +------+-------+---+------+
                                                  //| |    11|Jackson| 21|     M|
                                                  //| |    12|   Emma| 25|     F|
                                                  //| +------+-------+---+------+
                                                  //| only showing top 2 rows
                                                  //| 
 
 //take
 val first2Rows = customer2DF.take(2)             //> first2Rows  : Array[org.apache.spark.sql.Row] = Array([11,Jackson,21,M], [1
                                                  //| 2,Emma,25,F])
 customer2DF.write.json("S:/AllData/new")         //> org.apache.spark.sql.AnalysisException: path file:/S:/AllData/new already e
                                                  //| xists.;
                                                  //| 	at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelation
                                                  //| Command.run(InsertIntoHadoopFsRelationCommand.scala:106)
                                                  //| 	at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffect
                                                  //| Result$lzycompute(commands.scala:58)
                                                  //| 	at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffect
                                                  //| Result(commands.scala:56)
                                                  //| 	at org.apache.spark.sql.execution.command.ExecutedCommandExec.doExecute(
                                                  //| commands.scala:74)
                                                  //| 	at org.apache.spark.sql.execution.SparkPlan$$anonfun$execute$1.apply(Spa
                                                  //| rkPlan.scala:117)
                                                  //| 	at org.apache.spark.sql.execution.SparkPlan$$anonfun$execute$1.apply(Spa
                                                  //| rkPlan.scala:117)
                                                  //| 	at org.apache.spark.sql.execution.SparkPlan$$anonfun$executeQuery$1.appl
                                                  //| y(SparkPlan.scala:138)
                                                  //| 	at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.s
                                                  //| cala:151)
                                                  //| 	at org.apac
                                                  //| Output exceeds cutoff limit.
 //customerDF.write
//.format("org.apache.spark.sql.json")
//.save("S:/AllData/j")







}