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
    val listEvent = List(("sumar",-10),("restar", 2), ("restar", -3), ("sumar", 4))
    val newBalance = listEvent.foldLeft(person)((a, b) => changeStatus(a, b)).balance
    println("This is the final balance: " + newBalance)
    sc.stop()

  }

  def changeStatus(personStatus: Person, eventValue: (String, Int)): Person = {
    var balance = personStatus.balance
    if(eventValue._1 == "sumar"){
      balance += eventValue._2
    } else {
      balance -= eventValue._2
    }
    Person(balance)
  }
}
