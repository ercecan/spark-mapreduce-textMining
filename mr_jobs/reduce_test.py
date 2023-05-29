#!/usr/bin/env python

import sys

# Define the Reduce function
def reducer(key, values):
    # Count the occurrences of the word
    count = sum(map(int, values))
    # Output the word and its count
    print(key + "\t" + str(count))
