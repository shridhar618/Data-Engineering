# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session (FIX for Mac)
spark = SparkSession.builder \
    .appName("Movies Analysis") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

# -------------------------------
# Load dataset
# -------------------------------
df = spark.read.csv(
    "file:///Users/shridharbhat/Documents/Data Engineering/SparkSQL/movies.txt",
    header=True,
    inferSchema=True
)

df.show()

# -------------------------------
# Sorting
# -------------------------------
df.orderBy('Collection', ascending=False).show(5)
df.orderBy('imdb', ascending=False).show(5)

# -------------------------------
# Aggregations
# -------------------------------
df.agg({'Collection':'sum'}).show()
df.agg({'Collection':'avg'}).show()
df.agg({'Collection':'avg', 'imdb':'avg'}).show()

# -------------------------------
# Describe
# -------------------------------
df.describe().show()

# -------------------------------
# Group By
# -------------------------------
df.groupBy('Genre').agg(max('Collection')).show()

# -------------------------------
# Filtering
# -------------------------------
df.filter(df['Genre'] == 'Action').show()

df.filter(
    (df['Genre'] == 'Action') & 
    (df['imdb'] > 8)
).show()

df.filter(
    (~df['Genre'].isin('Action', 'Sci-Fi')) & 
    (df['imdb'] > 8)
).show()

# -------------------------------
# Range filter (inclusive)
# -------------------------------
fil_res = df.filter(df['Collection'].between(500, 2000))
fil_res.show()

# -------------------------------
# Add constant column
# -------------------------------
df.withColumn('Critical_hit', lit('YES')).show()

# -------------------------------
# Conditional column (when)
# -------------------------------
df2 = df.withColumn(
    'Critic_hit',
    when(df['imdb'] >= 8, 'YES').otherwise('NO')
)

df2.show()

# -------------------------------
# Group by new column
# -------------------------------
df2.groupBy('Critic_hit').count().show()

# -------------------------------
# Multi-condition when (FIXED)
# -------------------------------
x = (
    when(df['Collection'] >= 2000, 'Blockbuster')
    .when(df['Collection'] >= 1500, 'Huge Hit')
    .when(df['Collection'] >= 1000, 'Big Hit')
    .otherwise('Hit')
)

result = df.withColumn('BoxOffice', x)
result.show()

# -------------------------------
# UDF (FIXED LOGIC)
# -------------------------------
def box_office_verdict(coll):
    if coll >= 2000:
        return 'Blockbuster'
    elif coll >= 1500:
        return 'Huge Hit'
    elif coll >= 1000:
        return 'Big Hit'
    else:
        return 'Hit'

box_office_udf = udf(box_office_verdict)

result_udf = df.withColumn('BoxOffice', box_office_udf(df['Collection']))
result_udf.show()

# Stop Spark
spark.stop()