import BigMartSalesDataAnalysis.Data
import breeze.linalg.{Counter, sum}
import breeze.numerics.round
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_list, lit}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.json4s.scalap.scalasig.ClassFileParser.annotations

object BigMartSalesDataAnalysis extends App{

  case class SalesData(item_Identifier : String, Item_Fat_Content : String,
                       Item_Type:String,Item_MRP :Double,
                       Outlet_Identifier : String,Outlet_Size : String,
                       Outlet_Location_Type : String,Outlet_Type : String,
                       Gender:String,Age : Int,Profession : String , Item_Outlet_Sales:Double)

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "Dataset")
  val spark = SparkSession
    .builder
    .master("Local[*]")
    .appName("Test")
    .getOrCreate()

  val SalesDataSchema = new StructType()
    .add("item_Identifier",StringType)
    .add("Item_Fat_Content",StringType)
    .add("Item_Type",StringType)
    .add("Item_MRP",DoubleType)
    .add("Outlet_Identifier",DoubleType)
    .add("Outlet_Size",StringType)
    .add("Outlet_Location_Type",IntegerType)
    .add("Outlet_Type",StringType)
    .add("Gender",StringType)
    .add("Age",IntegerType)
    .add("Profession",DoubleType)
    .add("Item_Outlet_Sales",DoubleType)


  import spark.implicits._
  val Data = spark.read
    .option("header", "true")
    //.schema(SalesDataSchema)
    .option("inferSchema", "true")
    .csv("data/BigMartSales.csv")
   //.as[SalesData]



  Data.show(2)

  /* KPI1 : Which products are being sold most.
     So,that seller will concentrate more on improving the quality of products. */
    println(" Which products are being sold most So,that seller will concentrate more on improving the quality of products.")
    Data.groupBy("Item_Type").count().orderBy($"count".desc).show(10,false)

  // KPI2 : which products are being sold less. So, they will be able give discounts on products to improve their business.
    println("which products are being sold less. So, they will be able give discounts on products to improve their business.")
    Data.groupBy("Item_Type").count().orderBy($"count".asc).show(10)

    /* KPI3 : which age group of customers interested in which product.
   Based on this analysis, sellers will advertise products to that age group using channels like social media.*/
    //val interval = 20
     //Data.groupBy("Age","Item_Type").count().show(10)
 println("which age group of customers interested in which product.Based on this analysis, sellers will advertise products to that age group using channels like social media.")
  val interval = 10
  val Data1 = Data.withColumn("Age-range", $"Age" - ($"Age" % interval))
    .withColumn("Age-range", concat($"Age-range", lit(" - "), $"Age-range" + interval)) //optional one
  //println("SEE THIS OUTPUT")
  //Data1.select("Age-range","Age").show

 val test = Data1.groupBy("Age-range","Item_Type").count().orderBy($"Age-range".asc,$"count".desc)
  //val test = Data1.groupBy("Age-range").agg(count("Item_Type")as("Count")).orderBy($"Age-range".asc,$"count".desc)
  //test.select("Age-range","Item_Type").show(false)
  //test.select("Age-range","Count").show(false)
  val w = Window.partitionBy("Age-range").orderBy($"Age-range".asc,$"count".desc)
  val result = test.withColumn("index", row_number().over(w))

  result.orderBy($"Age-range".asc,$"count".desc).filter($"index"<=5 ).show(false)
  //result.filter($"index"<=3 ).show(false)






  // KPI4 : Total earnings of each outlet type(ex:supermarket, grocery store).
  println("Total earnings of each outlet type(ex:supermarket, grocery store)")
  val test12 =    Data.groupBy("Outlet_Type").sum("Item_Outlet_Sales")
  test12.withColumnRenamed("sum(Item_Outlet_Sales)","Earnings").show()




  // KPI5 : Which gender is interested in which product.
     //val data2 = Data.groupBy("Gender", "Item_Type").count().show(10, true)
  println("Which gender is interested in which product.")
  val lines1 = Data.groupBy("Gender","Item_Type").count().orderBy($"count".desc)

  val toptwo = Window.partitionBy("Gender").orderBy($"Gender".asc)
  val aggdf = lines1.withColumn("row",row_number.over(toptwo))
  aggdf.orderBy($"Gender".asc,$"count".desc).filter($"row"<=2 ).show(false)







}
