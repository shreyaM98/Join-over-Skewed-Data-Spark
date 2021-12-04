import org.apache.spark.sql.SparkSession

object ProjectApproach1{
  def main(args: Array[String]){
    if(args.length == 4){// checking command line arguments

      // creating Spark Session instance and running locally with as many worker threads as logical cores on machine
      val spark = SparkSession.builder.appName("Project-Approach-1").getOrCreate()

      // setting file paths
      val aPath = args(0)
      val bPath = args(1)
      val opPath = args(2)
      val repartnum = args(3).toInt

      // Create paired RDD of A Dataset (skewed - Transactions)
      val aRdd = spark.sparkContext.textFile(aPath)
      val aPairs = aRdd.map(line => line.split(",")).map(line => (line(0).trim.toInt,(line(1).trim.toInt, line(2), line(3))))
      val aPairs2 = aPairs.repartition(repartnum)//repartition to use parallelism is file is 1 block in hdfs
      // Create paired RDD of B Dataset (nonskewed - Customer)
      val bRdd = spark.sparkContext.textFile(bPath)
      val bPairs = bRdd.map(line => line.split(",")).map(line => (line(0).trim.toInt,(line(1), line(2).trim.toInt, line(3))))
      val bPairs2 = bPairs.repartition(repartnum)
      // Assuming skewed keys as 1 andd 2
      val skwKeysList = List(1,2)
      // Filtering B dataset to get only rows with skewed keys
      val skwKeyBRows = bPairs2.filter(b=> skwKeysList.contains(b._1))
      // Creating map
      val skwKeyBRowsMap = skwKeyBRows.collectAsMap()
      // Braodcasting rows with skewed keys
      val brdcastBRows = spark.sparkContext.broadcast(skwKeyBRowsMap)

      // Broadcast Hash Join - joining A skewed key rows with broadcasted B rows
      val brdcstJoin = aPairs2.mapPartitions(iter => {
        iter.flatMap{
          case (k,v1 ) =>
            brdcastBRows.value.get(k) match {
              case None => Seq.empty
              case Some(v2) => Seq((k, (v1, v2)))
            }
        }
      }, preservesPartitioning = true)

      // Filter A to get rows with non-skewed key
      val filteredA = aPairs2.filter(x => !skwKeyBRowsMap.keys.toList.contains(x._1))

      // Normal join betweeen A and B without skewed key rows
      val abJoin = filteredA.join(bPairs2)

      // Unioning both join results
      val result = brdcstJoin.union(abJoin)

      //writing output into output folder
      result.saveAsTextFile(opPath)

      spark.stop()
    }
    else{
      println("Require four arguments specifying input files, output, and number of partitions!!")
    }
  }
}