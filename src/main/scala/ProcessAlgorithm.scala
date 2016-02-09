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
    val person = Person(0)

    // We receive the list of events, in this case just a set of number
    // to sum and substract
    val listEvent = List(("sumar",10),("restar", 20), ("restar", 3), ("sumar", 4))

    changeStatusRecursive(listEvent, person).foreach(println)

    sc.stop()

  }

  def changeStatusRecursive(eventList: List[(String, Int)], person: Person): List[(String, Int)] = {

    val accum = (Person(0.0),List(("start", 0)))
    val newBalance = eventList.foldLeft(accum)((a, b) => (changeStatus(a, b)))

    newBalance._2
  }

  def changeStatus(acc: (Person, List[(String, Int)]), eventValue: (String, Int)): (Person, List[(String, Int)]) = {
    val person = acc._1
    var eventList = acc._2

    var balance = person.balance
    if(eventValue._1 == "sumar"){
      balance += eventValue._2
      eventList = eventList :+ eventValue
    } else if (eventValue._1 == "restar" && person.balance > eventValue._2){
      balance -= eventValue._2
      eventList = eventList :+ eventValue
    }
    (Person(balance), eventList)
  }

}
