
/** Wikipedia Logistic regression example
Project: Test
ws : eclipsewsOLD
sc logisticWikipedia
looking for a simple logistic past HOsmer CHD data.
"A group of 20 students  spend between 0-6 hours studyiig for an
exam. How does number of hours affect probabity that student passes"?

2018-03-21
rr
*/

package apps
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.Row

import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, explode, length, split, substring}

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg.{ Vectors, Vector}
import org.apache.spark.ml.classification.LogisticRegression

object Asgn11 {
type D = Double
type V = Vector
type S = String
type I = Int
Logger.getLogger("org").setLevel(Level.OFF)
val spark = SparkSession.builder
             .master("local[*]")
             .appName("Asgn11")
             .getOrCreate()                       //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.proper
                                                  //| ties
                                                  //| spark  : org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSess
                                                  //| ion@7cb8437d
 import spark.implicits._
 println(s" Spark version , ${spark.version} ")   //>  Spark version , 2.2.0 
 //val sc = spark.sparkContext
//val X = Vectors.dense(.5, .75, 1.0, 1.25, 1.5, 1.75, 1.75, 2.0, 2.25, 2.50, 2.75, 3.0, 3.25,
//3.5, 4.0, 4.25, 4.5, 4.75, 5.0, 5.5 )
val XHours= Vector(.5, .75, 1.0, 1.25, 1.5, 1.75, 1.75, 2.0, 2.25, 2.50, 2.75, 3.0, 3.25,
3.5, 4.0, 4.25, 4.5, 4.75, 5.0, 5.5 )             //> XHours  : scala.collection.immutable.Vector[Double] = Vector(0.5, 0.75, 1.0
                                                  //| , 1.25, 1.5, 1.75, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0, 3.25, 3.5, 4.0, 4.25, 4
                                                  //| .5, 4.75, 5.0, 5.5)
val YPass = Vector(0.0,0,0,0,0,0, 1,0,1,0,1,0,1,0,1, 1,1,1,1,1)
                                                  //> YPass  : scala.collection.immutable.Vector[Double] = Vector(0.0, 0.0, 0.0, 
                                                  //| 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 
                                                  //| 1.0, 1.0)
 val pairs = YPass zip XHours                     //> pairs  : scala.collection.immutable.Vector[(Double, Double)] = Vector((0.0,
                                                  //| 0.5), (0.0,0.75), (0.0,1.0), (0.0,1.25), (0.0,1.5), (0.0,1.75), (1.0,1.75),
                                                  //|  (0.0,2.0), (1.0,2.25), (0.0,2.5), (1.0,2.75), (0.0,3.0), (1.0,3.25), (0.0,
                                                  //| 3.5), (1.0,4.0), (1.0,4.25), (1.0,4.5), (1.0,4.75), (1.0,5.0), (1.0,5.5))
 val temp = pairs.map{ case (y,x)=>
    (y, Vectors.dense(x))
    }                                             //> temp  : scala.collection.immutable.Vector[(Double, org.apache.spark.ml.lina
                                                  //| lg.Vector)] = Vector((0.0,[0.5]), (0.0,[0.75]), (0.0,[1.0]), (0.0,[1.25]), 
                                                  //| (0.0,[1.5]), (0.0,[1.75]), (1.0,[1.75]), (0.0,[2.0]), (1.0,[2.25]), (0.0,[2
                                                  //| .5]), (1.0,[2.75]), (0.0,[3.0]), (1.0,[3.25]), (0.0,[3.5]), (1.0,[4.0]), (1
                                                  //| .0,[4.25]), (1.0,[4.5]), (1.0,[4.75]), (1.0,[5.0]), (1.0,[5.5]))
 val trainingDF = spark.createDataFrame(temp)
     .toDF("label","features")                    //> trainingDF  : org.apache.spark.sql.DataFrame = [label: double, features: ve
                                                  //| ctor]
 trainingDF.show()                                //> +-----+--------+
                                                  //| |label|features|
                                                  //| +-----+--------+
                                                  //| |  0.0|   [0.5]|
                                                  //| |  0.0|  [0.75]|
                                                  //| |  0.0|   [1.0]|
                                                  //| |  0.0|  [1.25]|
                                                  //| |  0.0|   [1.5]|
                                                  //| |  0.0|  [1.75]|
                                                  //| |  1.0|  [1.75]|
                                                  //| |  0.0|   [2.0]|
                                                  //| |  1.0|  [2.25]|
                                                  //| |  0.0|   [2.5]|
                                                  //| |  1.0|  [2.75]|
                                                  //| |  0.0|   [3.0]|
                                                  //| |  1.0|  [3.25]|
                                                  //| |  0.0|   [3.5]|
                                                  //| |  1.0|   [4.0]|
                                                  //| |  1.0|  [4.25]|
                                                  //| |  1.0|   [4.5]|
                                                  //| |  1.0|  [4.75]|
                                                  //| |  1.0|   [5.0]|
                                                  //| |  1.0|   [5.5]|
                                                  //| +-----+--------+
                                                  //| 
 val logReg = new LogisticRegression()            //> logReg  : org.apache.spark.ml.classification.LogisticRegression = logreg_3b
                                                  //| 74ce367251
 val logRegModel = logReg.fit(trainingDF)         //> [Stage 0:>                                                          (0 + 0
                                                  //| ) / 4]                                                                    
                                                  //|             18/03/23 19:39:00 WARN BLAS: Failed to load implementation fro
                                                  //| m: com.github.fommil.netlib.NativeSystemBLAS
                                                  //| 18/03/23 19:39:01 INFO JniLoader: successfully loaded C:\Users\Suraj\AppDat
                                                  //| a\Local\Temp\jniloader8023019642787104789netlib-native_ref-win-x86_64.dll
                                                  //| 18/03/23 19:39:01 INFO LBFGS: Step Size: 1.000
                                                  //| 18/03/23 19:39:01 INFO LBFGS: Val and Grad Norm: 0.639833 (rel: 0.0769) 0.1
                                                  //| 46980
                                                  //| 18/03/23 19:39:01 INFO LBFGS: Step Size: 2.250
                                                  //| 18/03/23 19:39:01 INFO LBFGS: Val and Grad Norm: 0.603065 (rel: 0.0575) 0.1
                                                  //| 26352
                                                  //| 18/03/23 19:39:01 INFO LBFGS: Step Size: 1.000
                                                  //| 18/03/23 19:39:01 INFO LBFGS: Val and Grad Norm: 0.420498 (rel: 0.303) 0.05
                                                  //| 82642
                                                  //| 18/03/23 19:39:01 INFO LBFGS: Step Size: 1.000
                                                  //| 18/03/23 19:39:01 INFO LBFGS: Val and Grad Norm: 
                                                  //| Output exceeds cutoff limit.
                                                              
 //val pipeline = new Pipeline().setStages(Array(logReg))
 //val linRegModel = pipeline.fit(trainingDF)
 println(s"Coefficients: ${logRegModel.coefficients} intercept: ${logRegModel.intercept}")
                                                  //> Coefficients: [1.5046457892761336] intercept: -4.077712989196636
 
 
 
 }// logisticWikipedia
 
 
 
 
/*
val trainingData = spark.createDataFrame(Seq(
(6.0, Vectors.dense(3.0)),
(9.0, Vectors.dense(4.0)),
(15.0, Vectors.dense(5.0))
)).toDF("label", "features")      // this created a DataFrame



 	Coefficient 	Std.Error 	z-value 	P-value (Wald)
Intercept 	−4.0777 	1.7610 	−2.316 	0.0206
Hours 	1.5046 	0.6287 	2.393 	0.0167
The output indicates that hours studying is significantly
associated with the probability of passing the exam ( p = 0.0167
{\displaystyle p=0.0167} {\displaystyle p=0.0167}, Wald test).
The output also provides the coefficients for Intercept = − 4.0777
 {\displaystyle {\text{Intercept}}=-4.0777} {\displaystyle
 {\text{Intercept}}=-4.0777} and Hours = 1.5046 {\displaystyle
 {\text{Hours}}=1.5046} {\displaystyle {\text{Hours}}=1.5046}.
 These coefficients are entered in the logistic regression equation
  to estimate the probability of passing the exam

*/