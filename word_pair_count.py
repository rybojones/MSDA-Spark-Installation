import re
from itertools import islice
from pyspark import SparkContext, SparkConf
from collections import Counter, OrderedDict

# change these filepaths as needed
INPUT_FILEPATH = './input.txt'
OUTPUT_FILEPATH = './my_output.txt'


# used to print the first-n items in the dict below
# borrowed from https://stackoverflow.com/questions/7971618/return-first-n-keyvalue-pairs-from-dict
def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))


#Take in a list of strings and output a Counter dictionary with word-pair tuple counts.
def pair_counter(list_line):

    # define a Counter dict with counts of individual tokens in list_line.
    # this will be used to define the counts for identical word-pairs
    counter_matches = Counter(list_line)

    # define a Counter dict that contains word-pair counts
    counter_dict = Counter()

    # define a set or tokens in list_lines, i.e. the unique tokens
    set_check = set(list_line)

    # iterate through the tokens in list_line
    for i in range(len(list_line)):
        # iterate through the words in list_line a second time
        for j in range(len(list_line)):
            # continue without changes if the indices match
            if i == j:
                continue
            # if the word-pair is two identical tokens, use the counter_matches dict
            # defined above to count the number of occurrences
            if list_line[i] == list_line[j]:
                # the number of occurrences minus one (don't include the token in-hand)
                counter_dict[(list_line[i], list_line[j])] = counter_matches[list_line[i]] - 1
            # check that the token from the first iteration has not already been used in a previous step
            if list_line[i] in set_check:
                # increase the count on the word-pair by one
                counter_dict[(list_line[i], list_line[j])] += 1
            # continue without changes if the token has already been used
            else:
                continue
        # remove the token from the check set to ensure it isn't counted multiple times
        if list_line[i] in set_check:
            set_check.remove(list_line[i])
        else:
            continue
    # return the Counter dict with counts of word-pairs
    return counter_dict


# configure and start spark processes/objects
conf = SparkConf().setAppName("WordPairCount").setMaster("local[*]").set("spark.ui.port", "6066")
sc = SparkContext(conf=conf)


# load the input text file
text = sc.textFile(INPUT_FILEPATH)


# perform parallelized operations on text to produce word-pair counts
#   the regex excludes all non-alpha and whitespace characters
#   use RDD object to convert to lowercase, alpha characters with whitespace
#   split the lines into groups of words
#   run the lines of tokens thru pair_counter()
#   aggregate the word-pair counts
regex = re.compile("[^a-zA-Z\s]")
pair_counts = text.map(lambda x: x.lower())\
        .map(lambda x: regex.sub("", x))\
        .map(lambda x: x.split())\
        .map(lambda x: pair_counter(x))\
        .reduce(lambda x, y: x + y)


# solution dictionary ordered by pair counts in descending order
dict_solution = {k: v for k, v in sorted(pair_counts.items(), key=lambda item: item[1], reverse=True)}


# write the word-pair count dictionary to file output.txt
with open(OUTPUT_FILEPATH, 'w') as f:
    print(dict_solution, file=f)


# view the first 25 most common word-pair occurrences
top_25 = take(25, dict_solution.items())
print('Top 25 Most Frequent Word-Pair Counts:\n')
for pair in top_25:
    print(pair[0], ':', pair[1])


# stop the current SparkContext
sc.stop()
