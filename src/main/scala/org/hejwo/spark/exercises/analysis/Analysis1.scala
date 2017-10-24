package org.hejwo.spark.exercises.analysis

import java.time.{LocalDate, Period}

import org.apache.spark.sql.{DataFrame, SparkSession}

object Analysis1Main extends Analysis1 {

  private val sql: SparkSession = SparkSession.builder().appName("Analysis1").master("local").getOrCreate()

  import sql.implicits._

  def main(args: Array[String]): Unit = {
    val persons = makeDF()

    persons
      .select("pesel", "name", "surname")
      .where($"city" === "Lublin").orderBy("pesel")
      .show()
  }

  private def makeDF(): DataFrame = {
    val persons: List[Person] = List(
      Person(1, "Adam", "Adamczewski", LocalDate.of(1987, 10, 3), "Lublin"),
      Person(2, "Szczepan", "Szczepan", LocalDate.of(1992, 4, 3), "Lublin")
    )
    sql.sparkContext.setLogLevel("WARN")
    sql.createDataFrame(persons)
  }
}

/**
  * Given 3 data sets write queries for :
  * 1. Grouping people in age categories : adult, child, senior using pattern matching
  * 2. Find passion of given person
  *
  * - Think about partitioning and size of data sets that you are creating
  * - Use .dependencies to trace whole process
  * - Use .toDebugString to find data sufferings
  */
class Analysis1 {


}



object Person {
  def apply(pesel: Int, name: String, surname: String, birthDate: LocalDate, city: String): Person = {
    val age = Period.between(birthDate, LocalDate.now()).getYears
    Person(pesel, name, surname, age, city)
  }
}
case class Person(pesel: Int, name: String, surname: String, age: Int, city: String)

case class Passion(pesel: Int, name: String)

case class JobTitle(pesel: Int, name: String, category: String)
