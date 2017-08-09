/* <<<<<<<<<<<<<<-------------- QUERIES ----------------->>>>>>>>>>>>>>>
Using spark-sql, Find:
1. What are the total number of gold medal winners every year
2. How many silver medals have been won by USA in each sport
*/

import org.apache.spark.sql.{Row,Column,SparkSession,SQLContext}  //Explanation is already given in Assignment18.1

object Session19Assign1 extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Session19Assign1")
    .config("spark.sql.warehouse.dir", "file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 19/Assignments/Assignment1")
    .getOrCreate()
  //Explanation is already given in Assignment 18.1

  val df1 = spark.sqlContext.read.option("header","true").csv("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 19/Assignments/Assignment1/Sports_data.txt")
  /* Explanation
  -> to read text file, built-in csv api is used along with option, where header is mentioned as true because file contains headers
  -> [df1 -->> sql.DataFrame] is created as and when a file is read
  */

  df1.printSchema()       //prints schema of DataFrame
  //REFER Screenshot 1 for output

  df1.show(30)            //shows data inside DataFrame in tabular format
  //REFER Screenshot 2 for output

  //<<<<<<<<<--------- Creation of case class ------------>>>>>>>>>>>
  import spark.implicits._          //to use case class import is required
  case class df1Class(firstname:String,lastname:String,sports:String,medal_type:String,age:Int,year:Int,country:String)
  //case class "df1Class" is created to provide specific data types to df1 DataFrame, where all data is received as String

  val df2 = df1.map{
    case Row(
    a:String,
    b:String,
    c:String,
    d:String,
    e:String,
    f:String,
    g:String
    ) => df1Class(firstname=a,lastname=b,sports=c,medal_type=d,age=e.toInt,year=f.toInt,country=g)
  }
  /*Explanation
  -> df1 is mapped to case class
  -> df2 -->> Dataset[Session19Assign1.df1Class] is created which now contains type specific data
  */


  df2.printSchema()         //prints schema
  //REFER Screenshot 3 for output

  df2.show()                //shows data in tabular format
  //REFER Screenshot 4 for output

  df2.createOrReplaceTempView("sportsTable")         //sportsTable is a temporary view created from df2

  spark.sql("select * from sportsTable").show(30)         //select query fetches the data from sportsTable and using show() data is shown in tabular format
  //REFER Screenshot 5 for output



  //<<<<<<<<<<<<<<------------------- QUERY 1 ------------------------->>>>>>>>>>>>>>>>>>>
  //1. What are the total number of gold medal winners every year

  spark.sql("select year,count(medal_type) as goldMedalWinners from sportsTable" +
    " where medal_type = 'gold'" +
    " group by year" +
    " order by year").show()

  /*Explanation
  -> select query fetches year and does count(medal_type) depending on where condition and group by clause from sportsTable
  -> result of select query is sorted using order by clause in ascending order (default order)
  -> finally result is shown in tabular format
   */
  //REFER Screenshot 6 for output


  //2. How many silver medals have been won by USA in each sport

  spark.sql("select sports,count(medal_type) as silverMedalByUSA from sportsTable" +
    " where medal_type = 'silver'" +
    " and country = 'USA'" +
    " group by sports" +
    " order by sports").show()

  /*Explanation
  -> select query fetches sports and does count(medal_type) depending on where condition and group by clause from sportsTable
  -> result of select query is sorted using order by clause in ascending order (default order)
  -> finally result is shown in tabular format
  */
  //REFER Screenshot 7 for output


}
