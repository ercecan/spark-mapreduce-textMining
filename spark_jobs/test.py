from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import en_core_web_sm
from time import time

nlp = en_core_web_sm.load()

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL ner example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

file_name = '/Users/erce/Desktop/ITU/Advanced Database Systems/TextMiningBenchmark/data/vit_b_abstracts.txt'
t = time()
df = spark.read.text(file_name)
t_e = time()
print(f'{t_e - t} seconds for {file_name} on read')
# t = time()
# res = nlp(df['value'])
# t_df = time()
# print(t_df-t, ' seconds for complete')

# t=time()
# itrable=df.rdd.toLocalIterator()
# for row in itrable:
#     res = nlp(row[0])
# t_for=time()
# print(t_for-t, ' seconds for complete')
def ner(text):
    doc = nlp(text)
    entities = []
    for ent in doc.ents:
        tpl = (ent.text, ent.label_)
        entities.append(tpl)
    return entities

ner_udf = F.udf(ner, T.ArrayType(T.StructType([
    T.StructField("text", T.StringType()),
    T.StructField("label", T.StringType())
])))

# [{txt,lbl},{txt,lbl}]

t = time()
df_ner = df.withColumn("entities", ner_udf(df["value"]))
t_e = time()
print(f'{t_e - t} seconds for {file_name} on process')

# Display the results
df_ner.show(truncate=False, n=6)