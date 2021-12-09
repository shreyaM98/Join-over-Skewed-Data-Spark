import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

object NaiveApproach{
  val spark = SparkSession.builder.appName("BigDataFinalProject").config("spark.master", "local").getOrCreate()

  def getPartitionedRddA(filePath: String, repartitionNum: Int): RDD[(Int, (Int, Int, String, Int))] = {
    val rdd = spark.sparkContext.textFile(filePath)
    val pairs = rdd.map(line => line.split(",")).map(line => (line(0).trim.toInt,(line(1).trim.toInt, line(2).trim.toInt, line(3), line(4).trim.toInt)))
    pairs.repartition(repartitionNum)
  }

  def getPartitionedRddB(filePath: String, repartitionNum: Int): RDD[(Int, (String, Int, String, Int, Float))] = {
    val rdd = spark.sparkContext.textFile(filePath)
    val pairs = rdd.map(line => line.split(",")).map(line => (line(0).trim.toInt,(line(1), line(2).trim.toInt, line(3), line(4).trim.toInt, line(5).trim.toFloat)))
    pairs.repartition(repartitionNum)
  }

  def main(args: Array[String]){
    try {

      val fileAPath = args(0)
      val fileBPath = args(1)
      val outputFile = args(2)
      val repartitionNum = args(3).toInt

      val outputPath = new Path(outputFile)
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      if(fs.exists(outputPath) && fs.isDirectory(outputPath))
        fs.delete(outputPath,true)

      val pairsA = getPartitionedRddA(fileAPath, repartitionNum)
      val pairsB = getPartitionedRddB(fileBPath, repartitionNum)

      val naiveJoin = pairsA.join(pairsB)

      naiveJoin.saveAsTextFile(outputFile)
      spark.stop()
    }
    catch
    {
      case e : Exception => println(e.printStackTrace())
    }
  }
}