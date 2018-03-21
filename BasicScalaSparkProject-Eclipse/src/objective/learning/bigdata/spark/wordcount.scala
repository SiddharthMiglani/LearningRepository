/*
 * This is kind of the hello world program of spark
 * Code copied from "https://data-flair.training/blogs/create-spark-scala-project/"
 * 
 * It reads a text file and counts the occurence of all the words in the file
 * Note: If you use csv or tsv as input change the separately accordingly in following line:
 * val words = rawData.flatMap(line => line.split(" "))
 * 
 * This project doesn't use any build tool like Maven or SBT, and
 * hence all references to Spark libraries are local 
 * 
 * Add Spark Libraries to the build path to compile it successfully
 * 
 * Also, this program won't run directly from IDE, it is made to run
 * via jar using spark-submit
 * 
 * Go to spark installation's bin directory and submit spark job using
 * spark-submit.cmd utility as below:
 * 
 * "spark-submit --class objective.learning.bigdata.spark.wordcount --master local <Path to Jar>\SparkWordCount.jar inputFilePath OutputFilePath"
 */
package objective.learning.bigdata.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object wordcount {
	def main(args:Array[String]){
	  
		//Create conf object
		val conf = new SparkConf().setAppName("WordCount")

		//create spark context object
		val sc = new SparkContext(conf)

		//Check whether sufficient params are supplied
		if (args.length < 2) {
			println("Usage: ScalaWordCount <input> <output>")
			System.exit(1)
		}

		//Read file and create RDD
		val rawData = sc.textFile(args(0))

		//convert the lines into words using flatMap operation
		val words = rawData.flatMap(line => line.split("\t"))

		//count the individual words using map and reduceByKey operation
		val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _)

		//Save the result
		wordCount.saveAsTextFile(args(1))

		//stop the spark context
		sc.stop
	}
}