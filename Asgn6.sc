package apps

//import java.util.Date
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
//import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLImplicits
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.{DataFrame, Dataset}
// ML Feature Creation, Tuning, Models, and Model Evaluation
//import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
//import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
//import org.apache.spark.ml.evaluation.{RegressionEvaluator}
//import org.apache.spark.ml.regression.{LinearRegression}
//import org.apache.spark.ml.Pipeline
//import org.apache.spark.mllib.evaluation.RegressionMetrics
  

//case class MapEntryx(keyx: String, valuex: Integer)
//case class regressDataSet( name: String, age : Double, weight : Double, height : Double)
//case class Customer( id : Int , name : String, age : Long,gender: String, income : Double)

object Asgn6 {
Logger.getLogger("org").setLevel(Level.OFF)
val spark = SparkSession.builder
               .master("local[*]")
             .appName("IFT333FallNewBld")
             .getOrCreate()
  import spark.implicits._
 spark.version
 val sc = spark.sparkContext
  //file 1 with customer details
 	val csv = "S:/AllData/CustA6.csv"
	val customerHeaderRDD = sc.textFile(csv)
  //file 2 with customer details
  val csv2 = "S:/AllData/CustomersA6_2.txt"
	val customerRDD2 = sc.textFile(csv2)
	//customerRDD.collect()foreach(println)
	
	//creating an rdd without header
	val line1 = customerHeaderRDD.first
	val customerRDD =
  customerHeaderRDD.filter(line => line != line1)
  
  //map transforamtion
  val mapped = customerRDD.map{
                line => {
                 val a = line.split(",")
                  Customer( a(0).trim.toInt, a(1).trim,a(2).trim.toLong,a(3).trim, a(4).trim.toDouble)
                 }
               }
	mapped.collect()foreach(println)
 
 
 //filter transformation
 val filtered = customerRDD.filter{
 	l=>{
 		val a = l.split(",")
 		a(3)=='M'
 		}
 	}
 filtered.collect()foreach(println)


//flatmap
val sentence = "This is a sentence"
val s = sc.parallelize(Seq(sentence))
val words=s flatMap{l=>l.split(" ")}
words.collect()foreach(println)
  
//MapPartition
val animals =  sc.parallelize(
                     List(
                        "dog", "cat", "fish", "fox", "crow", "rat", "cow", "pig"
                     ),
                     4
                  )
val animals2 =  sc.parallelize(
                     List(
                        "dog", "cat", "fish", "rabbit", "hamster"
                     )
                  )
val wildAnimals =  sc.parallelize(
                     List(
                        "lion", "tiger", "bear", "cheeta", "panther"
                     )
                  )

val partition = animals.mapPartitionsWithIndex{(index, iterator)=> {
	val myList = iterator.toList
	myList.map(x => x + " -> " + index).iterator
	}
}
partition.collect()foreach(println)
	
   
    //union transformation
	val union = customerRDD.union(customerRDD2)
	union.collect()foreach(println)
	
	//val animalsUnion=animals.union(animals2)
	//animalsUnion.collect()foreach(println)
	
	
	//intersection
	val intersection = customerRDD.intersection(customerRDD2)
	intersection.collect().foreach(println)
	
	
	//subtract
	val subtract = customerRDD.subtract(customerRDD2)
	subtract.collect()foreach(println)
	
	
	//distinct
	val distinct = customerRDD2.distinct
	distinct.collect()foreach(println)
	
	
	//cartesian product
  val cartProd = animals2.cartesian(animals2)
  cartProd.collect()foreach(println)
  
  
  //zip
  val zip = animals2.zip(wildAnimals)
  zip.collect()foreach(println)
  
  
  //zip with index
	val zipIndexed = animals2.zipWithIndex()
  zipIndexed.collect()foreach(println)
  
  
  //group by
  val groupBy = mapped.groupBy { x => x.gender}
  groupBy.collect()foreach(println)
  
  
  //key by
 val keyBy = mapped.groupBy { x => x.gender}
  keyBy.collect()foreach(println)
  
	
	//sort by
	val sortBy = mapped.sortBy { x => x.name}
	sortBy.collect()foreach(println)
	
	
	//random split
	val split = customerRDD.randomSplit(Array(0.5,0.5))
	split(0).collect()foreach(println)
	split(1).collect()foreach(println)
//Repartition
val number = sc.parallelize((1 to 100).toList)
val parts = number.repartition(4)
val part=parts.partitions


//sample
val sample = number.sample(true, 0.1)
sample.collect()foreach(println)
	
}