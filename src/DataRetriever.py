from MongoDB_DAO import MongoDB_DAO


class DataRetriever:

    def __init__(self):
        pass

    def document_retriever(self, database_name):
        print("Retrieving tweets and articles ...")
        tweets_and_arcticles_list = []
        mongoDb = MongoDB_DAO(database_name)
        collections_list = mongoDb.collections_list()
        for collection_name in collections_list:
            for record in mongoDb.retrieve_data(collection_name):
                if database_name == "ProcessedDb":
                    keyword = "text"
                else:
                    keyword = "BODY"
                if keyword in record:
                    tweets_and_arcticles_list.append(record[keyword])
        return tweets_and_arcticles_list