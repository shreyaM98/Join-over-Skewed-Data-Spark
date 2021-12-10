import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SmartApproach{
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

        val startTime = System.nanoTime

        val pairsA = getPartitionedRddA(fileAPath, repartitionNum)
        val pairsB = getPartitionedRddB(fileBPath, repartitionNum)

        val skewedKeys = List(1, 2, 3) //Got the skewed list from the pyspark module
        val bSkewedRows = pairsB.filter(b => skewedKeys.contains(b._1))
        val mapSkewedRows = bSkewedRows.collectAsMap()
        val broadcastBRows = spark.sparkContext.broadcast(mapSkewedRows)

        val broadcastJoin = pairsA.mapPartitions(iteration => {
          iteration.flatMap {
            case (k, v1) =>
              broadcastBRows.value.get(k) match {
                case None => Seq.empty
                case Some(v2) => Seq((k, (v1, v2)))
              }
          }
        }, preservesPartitioning = true)

        val nonSkewedA = pairsA.filter(x => !mapSkewedRows.keys.toList.contains(x._1))
        val repartitionJoin = nonSkewedA.join(pairsB)

        val finalResult = broadcastJoin.union(repartitionJoin)
        finalResult.saveAsTextFile(outputFile)

        spark.stop()

        val duration = (System.nanoTime - startTime) / 1e9d
        println("Smart Approach time duration: " + duration)
      } catch {
        case e : Exception => println(e.printStackTrace())
      }
  }
}