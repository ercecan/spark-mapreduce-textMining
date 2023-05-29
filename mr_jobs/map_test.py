#!/usr/bin/env python

import sys
import string

# Define the list of stopwords
stopwords = ["the", "a", "an", "is", "of", "in", "this", "and", "that"]

# Define a translation table for removing punctuation
translator = str.maketrans('', '', string.punctuation)

# Define the Map function
def mapper(key, value):
    # Split the input text into words
    words = value.strip().split()
    # Remove punctuation and convert to lowercase
    words = [w.translate(translator).lower() for w in words]
    # Remove stopwords
    words = [w for w in words if w not in stopwords]
    # Output key-value pairs
    for word in words:
        print(word + "\t1")
