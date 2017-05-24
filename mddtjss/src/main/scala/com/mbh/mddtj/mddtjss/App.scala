package com.mbh.mddtj.mddtjss

import scala.io._
import scala.math.Integral
import scala.collection.mutable._



import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;
import java.io.File;
import java.io.FileWriter;


object App 
{
def main(args: Array[String]) : Unit = 
	{
    if (args.length != 2) 
    {  
      System.err.println("Usage: <input file> <output folder>")  
      
      System.exit(2);
    }  		
    
    val conf = new SparkConf().setAppName("mddtjss")
    val sc = new SparkContext(conf)
    
    
    val cf = new Configuration()
    // cf.set("fs.defaultFS", "hdfs://192.168.199.201:9000")
    val fs= FileSystem.get(cf)    
    
    val strFilePath = args(1) + "/result_.txt"
    var p = new Path(strFilePath)
  	if (fs.exists(p))   
  	{
  		fs.delete(p,true);
  	}        
		
		val mapcity = new HashMap[String, Set[Int]] with MultiMap[String, Int]
		val mapchannel = new HashMap[String, Set[Int]] with MultiMap[String, Int]
		val mapyearmonth = new HashMap[String, Set[Int]] with MultiMap[String, Int]
		val mapapp = new HashMap[String, Set[Int]] with MultiMap[String, Int]
		val mapcitychannel = new HashMap[String, Set[Int]] with MultiMap[String, Int]    
    
		val file = sc.textFile(args(0))
    file.collect().foreach(line => {
      val info = line.split(",")      
		  mapcity.addBinding(info(0), info(4).toInt)
		  mapchannel.addBinding(info(1), info(4).toInt)
		  mapyearmonth.addBinding(info(2), info(4).toInt)
		  mapapp.addBinding(info(3), info(4).toInt)
		  mapcitychannel.addBinding(info(0) + "-" + info(1), info(4).toInt)		
    }) 
    
    val mycity = mapcity.map(x => (x._1, x._2.sum))		
    mycity.foreach{println}
    
		val mychannel = mapchannel.map(x => (x._1, x._2.sum))		
		val myyearmonth = mapyearmonth.map(x => (x._1, x._2.sum))		
		val myapp = mapapp.map(x => (x._1, x._2.sum))		
		val mycitychannel = mapcitychannel.map(x => (x._1, x._2.sum))
		
    val output = fs.create(p, true)
		val writer = new PrintWriter(output, true)

		println("write file...")
	  mycity.foreach{case (k,v) => writer.append(k + " " + v + "\n")}
		mychannel.foreach{case (k,v) => writer.append(k + " " + v + "\n")}
		myyearmonth.foreach{case (k,v) => writer.append(k + " " + v + "\n")}
		myapp.foreach{case (k,v) => writer.append(k + " " + v + "\n")}
		mycitychannel.foreach{case (k,v) => writer.append(k + " " + v + "\n")}
	  writer.flush()
		writer.close()
		
		sc.stop()  
	}
}
