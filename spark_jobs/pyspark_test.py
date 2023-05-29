# Import necessary libraries
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.feature import CountVectorizer, Tokenizer, StopWordsRemover
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Create SparkContext and SparkSession
conf = SparkConf().setAppName("text-mining").setMaster("local")
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

# Load text data as DataFrame
text_data = [
    ("the quick brown fox", 0),
    ("jumped over the lazy dog", 1),
    ("the lazy dog slept", 0),
    ("the quick brown fox jumped over the lazy dog", 1)
]
df = spark.createDataFrame(text_data, ["text", "label"])

# Preprocess data by tokenizing and removing stopwords
stopwords = ["the", "a", "an", "is", "of", "in", "this", "and", "that"]
tokenizer = Tokenizer(inputCol="text", outputCol="words")
stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stopwords)
preprocessed_data = stopwords_remover.transform(tokenizer.transform(df))

# Extract features using CountVectorizer
cv = CountVectorizer(inputCol="filtered_words", outputCol="features")
model = cv.fit(preprocessed_data)
features = model.transform(preprocessed_data)

# Split the data into training and test sets
(training_data, test_data) = features.randomSplit([0.7, 0.3], seed=100)

# Train a Naive Bayes classifier on the training data
nb = NaiveBayes()
nb_model = nb.fit(training_data)

# Make predictions on the test data
predictions = nb_model.transform(test_data)

# Evaluate the performance of the classifier
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="label", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy:", accuracy)
