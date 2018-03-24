import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{StringIndexer}
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineStage, Transformer}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.regression.{LinearRegression}
import org.apache.spark.mllib.evaluation.RegressionMetrics


object quiz10LR {
  Logger.getLogger("org").setLevel(Level.OFF)
 val spark=SparkSession.builder().master("local[*]").appName("quiz10LR").getOrCreate()
                                                  //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.propert
                                                  //| ies
                                                  //| spark  : org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSessi
                                                  //| on@4fad6218
val fn = "S:/Course work/Spring 18/Analysing big data - 598/Assignments/Quiz10/LRDataSet.txt"
                                                  //> fn  : String = S:/Course work/Spring 18/Analysing big data - 598/Assignment
                                                  //| s/Quiz10/LRDataSet.txt
import spark.implicits._
val sc = spark.sparkContext                       //> sc  : org.apache.spark.SparkContext = org.apache.spark.SparkContext@1bf39d0
                                                  //| 6
val dataRDD = sc.textFile(fn)                     //> dataRDD  : org.apache.spark.rdd.RDD[String] = S:/Course work/Spring 18/Anal
                                                  //| ysing big data - 598/Assignments/Quiz10/LRDataSet.txt MapPartitionsRDD[1] a
                                                  //| t textFile at quiz10LR.scala:22
//dataRDD.collect()foreach(println)
val deliveryRDD = dataRDD.map{ line =>
    val ar = line.split("""\s+""")
    //val convert = if (ar(0)== "A") 1 else 0
    Minutes(ar(0).trim(),ar(1).trim.toInt, ar(2).trim.toInt, ar(3).trim.toDouble)
    }                                             //> deliveryRDD  : org.apache.spark.rdd.RDD[Minutes] = MapPartitionsRDD[2] at m
                                                  //| ap at quiz10LR.scala:24
    
val indexer = new StringIndexer()
      .setInputCol("region")
      .setOutputCol("indexedRegion")              //> indexer  : org.apache.spark.ml.feature.StringIndexer = strIdx_cb514e754abe
                                                  //| 
      
val deliveryDS = deliveryRDD.toDS                 //> deliveryDS  : org.apache.spark.sql.Dataset[Minutes] = [region: string, parc
                                                  //| els: int ... 2 more fields]

val indexedDS = indexer.fit(deliveryDS).transform(deliveryDS)
                                                  //> indexedDS  : org.apache.spark.sql.DataFrame = [region: string, parcels: int
                                                  //|  ... 3 more fields]
//indexedDS.show()
val typedDS= indexedDS
    .select($"minutes".cast(DoubleType).as("label"), $"parcels".cast(IntegerType) , $"ageTruck".cast(IntegerType),$"indexedRegion" )
                                                  //> typedDS  : org.apache.spark.sql.DataFrame = [label: double, parcels: int ..
                                                  //| . 2 more fields]
 val  assembler = new VectorAssembler()//creates a feature vector of the specified attributes
    .setInputCols(Array("parcels","indexedRegion","ageTruck"))
    .setOutputCol("features")                     //> assembler  : org.apache.spark.ml.feature.VectorAssembler = vecAssembler_9c7
                                                  //| 0911e75f7
  val outputDS = assembler.transform(typedDS)     //> outputDS  : org.apache.spark.sql.DataFrame = [label: double, parcels: int .
                                                  //| .. 3 more fields]
 //outputDS.show()
 val lr = new LinearRegression()                  //> lr  : org.apache.spark.ml.regression.LinearRegression = linReg_be1f2c31e95f
                                                  //| 
 val lrModel = lr.fit(outputDS)                   //> 18/03/19 21:28:25 WARN BLAS: Failed to load implementation from: com.github
                                                  //| .fommil.netlib.NativeSystemBLAS
                                                  //| [Stage 3:>                                                          (0 + 2
                                                  //| ) / 2]18/03/19 21:28:25 INFO JniLoader: successfully loaded C:\Users\Suraj\
                                                  //| AppData\Local\Temp\jniloader2701805990600936012netlib-native_ref-win-x86_64
                                                  //| .dll
                                                  //|                                                                           
                                                  //|       18/03/19 21:28:26 WARN LAPACK: Failed to load implementation from: c
                                                  //| om.github.fommil.netlib.NativeSystemLAPACK
                                                  //| 18/03/19 21:28:26 INFO JniLoader: already loaded netlib-native_ref-win-x86_
                                                  //| 64.dll
                                                  //| lrModel  : org.apache.spark.ml.regression.LinearRegressionModel = linReg_be
                                                  //| 1f2c31e95f
val coefficients = lrModel.coefficients           //> coefficients  : org.apache.spark.ml.linalg.Vector = [10.004923578947919,106
                                                  //| .13497262060633,3.264368338916422]
val intercept = lrModel.intercept                 //> intercept  : Double = -32.36067205794095
val rmse = lrModel.summary.rootMeanSquaredError   //> rmse  : Double = 20.465314760146235
val r2 = lrModel.summary.r2                       //> r2  : Double = 0.9546966338580712
//parcels=46, truckAge=1, region = A = 1,
val yhat = coefficients(0)*46+coefficients(1)*1+coefficients(2)*1
                                                  //> yhat  : Double = 569.625825591127
}
case class Minutes(region: String, parcels: Int, ageTruck: Int, minutes: Double)