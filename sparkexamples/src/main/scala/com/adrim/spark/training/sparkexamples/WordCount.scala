package com.adrim.spark.training.sparkexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
	def main(args: Array[String]) {
	val conf = new SparkConf().setAppName("Word Count").setMaster("local")
	
	val sc = new SparkContext(conf)
	
	if (args.length < 2) {
	 println("Missing Arguments !!!")
	}
	
	
	//Loading textfile into textFile object. (RDD)
	val textFile = sc.textFile(args(0))
	
	// read the line, split the line into words
	val words = textFile.flatMap(line=> line.split(" "))
	
	//Assign Count (1) to each word
	val counts = words.map(word=>(word,1))
	
	//Groupby key and Sum the list of values
	val wordcount =  counts.reduceByKey(_+_)
	
	//sort the result based on a Key
	val wordcount_sorted = wordcount.sortByKey()
	//wordcount_sorted.foreach(println)

	//store the output into file system
	wordcount_sorted.saveAsTextFile(args(1))
	
	}
	}
	