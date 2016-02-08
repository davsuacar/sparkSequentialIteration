import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.{RDD, AsyncRDDActions}
import org.apache.spark.{SparkContext, SparkConf}

import scala.concurrent.Future

/**
 * Created by davidsuarez on 3/02/16.
 */
object ProcessAlgorithm {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("ProcessAlgorithm")
    val sc = new SparkContext(conf)

    // Parallelize counter to be shared by all machines
    var count = Seq(10l, 0l)
    var listResult = List()
    var paralellizedCount = sc.parallelize(count)

    // We receive the list of events, in this case just a set of number
    // to sum and substract
    var listEvent = List(-10, 2, -3, 4)


    val futureList = sc.parallelize(listEvent)

    futureList.foreachAsync(changeStatus(sc, paralellizedCount, _))

//    listEvent.foldLeft(Future successful())((future, n) => future flatMap(_ => println(n)))

  }

  def changeStatus(sc: SparkContext, count: RDD[Long], event: Long) {
    val sum = count.sum()
    println(sum)
    if (sum > 0) {
      val seq = Seq[Long](event)
      count.union(sc.parallelize(seq))
    }
    count.foreach(println)
  }
}
