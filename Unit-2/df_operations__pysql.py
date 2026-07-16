
# PySpark DataFrame Operations Handbook (Single Script)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark import StorageLevel

spark = SparkSession.builder.appName("PySpark Handbook").getOrCreate()

# -----------------------------
# Sample Data
# -----------------------------
emp_data = [
    (101,"Rahul",22,"CS",45000),
    (102,"Anita",21,"IT",55000),
    (103,"Kiran",23,"CS",60000),
    (104,"Priya",22,"ECE",40000),
    (105,"Rohan",24,"IT",70000)
]
cols=["ID","Name","Age","Department","Salary"]

df = spark.createDataFrame(emp_data, cols)
df.createOrReplaceTempView("employee")

print("\n===== ORIGINAL DATA =====")
df.show()

# -----------------------------
# BASIC
# -----------------------------
df.show()
df.printSchema()
print(df.columns)
print(df.dtypes)

df.select("Name","Salary").show()
spark.sql("SELECT Name, Salary FROM employee").show()

df.filter(col("Salary")>50000).show()
spark.sql("SELECT * FROM employee WHERE Salary>50000").show()

df.where(col("Department")=="CS").show()
spark.sql("SELECT * FROM employee WHERE Department='CS'").show()

df.filter((col("Salary")>50000)&(col("Department")=="IT")).show()
spark.sql("""
SELECT * FROM employee
WHERE Salary>50000 AND Department='IT'
""").show()

# -----------------------------
# COLUMN OPERATIONS
# -----------------------------
df.withColumn("Bonus", col("Salary")*0.10).show()
spark.sql("SELECT *, Salary*0.10 AS Bonus FROM employee").show()

df.withColumn("Salary", col("Salary")+5000).show()
spark.sql("SELECT ID,Name,Age,Department,Salary+5000 AS Salary FROM employee").show()

df.withColumnRenamed("Salary","Income").show()
spark.sql("SELECT ID,Name,Age,Department,Salary AS Income FROM employee").show()

df.drop("Age").show()
spark.sql("SELECT ID,Name,Department,Salary FROM employee").show()

# -----------------------------
# SORT
# -----------------------------
df.orderBy("Salary").show()
spark.sql("SELECT * FROM employee ORDER BY Salary").show()

df.orderBy(col("Salary").desc()).show()
spark.sql("SELECT * FROM employee ORDER BY Salary DESC").show()

# -----------------------------
# DISTINCT / DUPLICATES
# -----------------------------
df.select("Department").distinct().show()
spark.sql("SELECT DISTINCT Department FROM employee").show()

df.dropDuplicates(["Department"]).show()

# -----------------------------
# LIMIT
# -----------------------------
df.limit(3).show()
spark.sql("SELECT * FROM employee LIMIT 3").show()

# -----------------------------
# AGGREGATION
# -----------------------------
df.groupBy("Department").agg(
    sum("Salary").alias("Total"),
    avg("Salary").alias("Average"),
    min("Salary").alias("Minimum"),
    max("Salary").alias("Maximum"),
    count("*").alias("Count")
).show()

spark.sql("""
SELECT Department,
SUM(Salary) Total,
AVG(Salary) Average,
MIN(Salary) Minimum,
MAX(Salary) Maximum,
COUNT(*) Count
FROM employee
GROUP BY Department
""").show()

# -----------------------------
# STRING FUNCTIONS
# -----------------------------
df.select(
    upper("Name").alias("UPPER"),
    lower("Name").alias("LOWER"),
    length("Name").alias("LEN"),
    substring("Name",1,3).alias("SUB")
).show()

spark.sql("""
SELECT
UPPER(Name),
LOWER(Name),
LENGTH(Name),
SUBSTRING(Name,1,3)
FROM employee
""").show()

# -----------------------------
# CONDITIONAL
# -----------------------------
df.withColumn(
    "Grade",
    when(col("Salary")>=60000,"High")
    .when(col("Salary")>=50000,"Medium")
    .otherwise("Low")
).show()

spark.sql("""
SELECT *,
CASE
WHEN Salary>=60000 THEN 'High'
WHEN Salary>=50000 THEN 'Medium'
ELSE 'Low'
END AS Grade
FROM employee
""").show()

# -----------------------------
# NULL HANDLING
# -----------------------------
df.na.fill(0).show()
df.na.drop().show()

# -----------------------------
# WINDOW
# -----------------------------
w=Window.partitionBy("Department").orderBy(col("Salary").desc())

df.withColumn("row_number",row_number().over(w))\
.withColumn("rank",rank().over(w))\
.withColumn("dense_rank",dense_rank().over(w))\
.show()

# -----------------------------
# JOIN
# -----------------------------
dept=[("CS","A"),("IT","B"),("ECE","C")]
deptdf=spark.createDataFrame(dept,["Department","Building"])
deptdf.createOrReplaceTempView("dept")

df.join(deptdf,"Department","inner").show()
spark.sql("""
SELECT *
FROM employee e
INNER JOIN dept d
ON e.Department=d.Department
""").show()

df.join(deptdf,"Department","left").show()
df.join(deptdf,"Department","right").show()
df.join(deptdf,"Department","outer").show()
df.join(deptdf,"Department","left_semi").show()
df.join(deptdf,"Department","left_anti").show()

# -----------------------------
# UNION
# -----------------------------
new=spark.createDataFrame([(106,"Deep",22,"CS",65000)],cols)
df.union(new).show()

# -----------------------------
# CACHE
# -----------------------------
df.cache()
df.persist(StorageLevel.MEMORY_ONLY)
df.unpersist()

# -----------------------------
# PARTITIONS
# -----------------------------
print(df.rdd.getNumPartitions())
df.repartition(4)
df.coalesce(1)

# -----------------------------
# FILES
# -----------------------------
# df.write.csv("csv_out",header=True)
# spark.read.csv("csv_out",header=True,inferSchema=True)

# df.write.json("json_out")
# spark.read.json("json_out")

# df.write.parquet("parquet_out")
# spark.read.parquet("parquet_out")

# -----------------------------
# CONVERSION
# -----------------------------
rdd=df.rdd
df2=rdd.toDF(cols)
df2.show()

spark.stop()
