from pyspark.sql.functions import col, lit, when

df_emp = spark.read.csv("file:///home/hadoop/emp1.csv", header=True, inferSchema=True)
df_dept = spark.read.csv("file:///home/hadoop/dept.csv", header=True, inferSchema=True)

df_emp = df_emp.withColumnRenamed("Epmid", "empid")

print("EMP DATA")
df_emp.show()

print("DEPT DATA")
df_dept.show()

print("INNER JOIN")
df_emp.join(df_dept, "empid", "inner").show()

print("LEFT JOIN")
l = df_emp.join(df_dept, "empid", "left")
l.show()

print("RIGHT JOIN")
df_emp.join(df_dept, "empid", "right").show()

print("FULL JOIN")
df_emp.join(df_dept, "empid", "full").show()

print("ADD CONSTANT COLUMN")
l1 = l.withColumn("newcol", lit(None))
l1.show()

print("CONDITIONAL COLUMN")
l2 = l.withColumn(
    "newcol",
    when(col("empid") == 101, lit("abcd")).otherwise(lit(None))
)
l2.show()

print("FILTER")
l.filter(col("empid") == 101).show()

print("SELECT")
l.select("empid", "name").show()