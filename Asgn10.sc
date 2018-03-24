package Assignment10
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.regression.{LinearRegression}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.math._
import org.apache.spark.mllib.stat.Statistics
import util.Random

object Asgn10 {


 Logger.getLogger("org").setLevel(Level.OFF)
 val spark=SparkSession.builder().master("local[*]").appName("Asgn10").getOrCreate()
                                                  //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| spark  : org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSessi
                                                  //| on@5ae95707
val file = "S:/Course work/Spring 18/Analysing big data - 598/Assignments/Asgn10/LRdata.txt"
                                                  //> file  : String = S:/Course work/Spring 18/Analysing big data - 598/Assignmen
                                                  //| ts/Asgn10/LRdata.txt
import spark.implicits._
val sc = spark.sparkContext                       //> sc  : org.apache.spark.SparkContext = org.apache.spark.SparkContext@62891fc8
                                                  //| 
val linesRDD = sc.textFile(file)                  //> linesRDD  : org.apache.spark.rdd.RDD[String] = S:/Course work/Spring 18/Anal
                                                  //| ysing big data - 598/Assignments/Asgn10/LRdata.txt MapPartitionsRDD[1] at te
                                                  //| xtFile at Assignment10.Asgn10.scala:25
val rowsRDD= linesRDD.map{ row => row.split(" ")}
       .map{cols => Row(cols(0).trim.toDouble, cols(1).trim.toDouble)
                    }                             //> rowsRDD  : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitio
                                                  //| nsRDD[3] at map at Assignment10.Asgn10.scala:27
rowsRDD.collect().foreach{ x => println(x)}       //> [1.0,4.0]
                                                  //| [1.0,7.0]
                                                  //| [3.0,9.0]
                                                  //| [3.0,12.0]
                                                  //| [4.0,11.0]
                                                  //| [4.0,12.0]
                                                  //| [5.0,17.0]
                                                  //| [6.0,13.0]
                                                  //| [6.0,18.0]
                                                  //| [7.0,17.0]
 val mySchema = StructType(Seq(
  StructField("attribute1", DoubleType, false),
  StructField("attribute2", DoubleType, false)
))                                                //> mySchema  : org.apache.spark.sql.types.StructType = StructType(StructField(
                                                  //| attribute1,DoubleType,false), StructField(attribute2,DoubleType,false))
 val df = spark. createDataFrame(rowsRDD,mySchema)//> df  : org.apache.spark.sql.DataFrame = [attribute1: double, attribute2: dou
                                                  //| ble]
 
 
 df.show()                                        //> +----------+----------+
                                                  //| |attribute1|attribute2|
                                                  //| +----------+----------+
                                                  //| |       1.0|       4.0|
                                                  //| |       1.0|       7.0|
                                                  //| |       3.0|       9.0|
                                                  //| |       3.0|      12.0|
                                                  //| |       4.0|      11.0|
                                                  //| |       4.0|      12.0|
                                                  //| |       5.0|      17.0|
                                                  //| |       6.0|      13.0|
                                                  //| |       6.0|      18.0|
                                                  //| |       7.0|      17.0|
                                                  //| +----------+----------+
                                                  //| 

val assembler = new VectorAssembler()
    .setInputCols(Array("attribute2"))
    .setOutputCol("features")                     //> assembler  : org.apache.spark.ml.feature.VectorAssembler = vecAssembler_143
                                                  //| 9a68e3dbe
                                                  
val sDF=assembler.transform(df)                   //> sDF  : org.apache.spark.sql.DataFrame = [attribute1: double, attribute2: do
                                                  //| uble ... 1 more field]
val finalDF=sDF.withColumnRenamed("attribute1", "label")
                                                  //> finalDF  : org.apache.spark.sql.DataFrame = [label: double, attribute2: dou
                                                  //| ble ... 1 more field]
finalDF.select("label","features").show()         //> +-----+--------+
                                                  //| |label|features|
                                                  //| +-----+--------+
                                                  //| |  1.0|   [4.0]|
                                                  //| |  1.0|   [7.0]|
                                                  //| |  3.0|   [9.0]|
                                                  //| |  3.0|  [12.0]|
                                                  //| |  4.0|  [11.0]|
                                                  //| |  4.0|  [12.0]|
                                                  //| |  5.0|  [17.0]|
                                                  //| |  6.0|  [13.0]|
                                                  //| |  6.0|  [18.0]|
                                                  //| |  7.0|  [17.0]|
                                                  //| +-----+--------+
                                                  //| 
val lr = new LinearRegression()                   //> lr  : org.apache.spark.ml.regression.LinearRegression = linReg_aeb42eca7af5
                                                  //| 
val lrModel = lr.fit(finalDF)                     //> 18/03/23 19:39:12 WARN BLAS: Failed to load implementation from: com.github
                                                  //| .fommil.netlib.NativeSystemBLAS
                                                  //| 18/03/23 19:39:12 INFO JniLoader: successfully loaded C:\Users\Suraj\AppDat
                                                  //| a\Local\Temp\jniloader4417119161554248976netlib-native_ref-win-x86_64.dll
                                                  //| 18/03/23 19:39:12 WARN LAPACK: Failed to load implementation from: com.gith
                                                  //| ub.fommil.netlib.NativeSystemLAPACK
                                                  //| 18/03/23 19:39:12 INFO JniLoader: already loaded netlib-native_ref-win-x86_
                                                  //| 64.dll
                                                  //| lrModel  : org.apache.spark.ml.regression.LinearRegressionModel = linReg_ae
                                                  //| b42eca7af5
val coefficients = lrModel.coefficients           //> coefficients  : org.apache.spark.ml.linalg.Vector = [0.4086021505376344]
val intercept = lrModel.intercept                 //> intercept  : Double = -0.9032258064516128
val trainingSummary = lrModel.summary             //> trainingSummary  : org.apache.spark.ml.regression.LinearRegressionTrainingS
                                                  //| ummary = org.apache.spark.ml.regression.LinearRegressionTrainingSummary@6f1
                                                  //| 7dd06
println(s"Coefficients : $coefficients, Intercept = $intercept")
                                                  //> Coefficients : [0.4086021505376344], Intercept = -0.9032258064516128
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
                                                  //> RMSE: 0.8334408532787305
println(s"r2: ${trainingSummary.r2}")             //> r2: 0.8172043010752689
 
}