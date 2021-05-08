from pyspark import SparkContext, SparkConf

from MapReducer import MapReducer


def init_map_reduce(database_name, filename):
    map_reduce = MapReducer(database_name)
    map_reduce.mapper()
    map_reduce.reducer(filename)


if __name__ == '__main__':
    conf = SparkConf().setMaster("spark://assignment-3.us-central1-a.c.organic-poetry-288820.internal:7077").setAppName(
        "MAP_REDUCER - WORD COUNT")
    SparkContext(conf=conf)

    # Map reduce for Tweets from ProcessedDb
    print("Map reduce for Tweets from ProcessedDb")
    init_map_reduce("ProcessedDb", "word_count_for_tweets.txt")

    # Map reduce for news articles in ReutersDb
    print("Map reduce for news articles in ReutersDb")
    init_map_reduce("ReuterDb", "word_count_for_articles.txt")
