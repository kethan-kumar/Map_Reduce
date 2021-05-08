from DataRetriever import DataRetriever


class MapReducer:

    def __init__(self,database_name):
        self.keywords = ["storm", "winter", "canada", "flu", "snow", "hot", "cold", "indoor", "safety", "rain", "ice"]
        self.word_map = []
        self.retrieved_documents = DataRetriever()
        self.documents_list = self.retrieved_documents.document_retriever(database_name)

    def mapper(self):
        print("Mapper: mapping words from tweets and articles ...")
        for text in self.documents_list:
            for word in text.split(" "):
                if self.isWordExists(word.lower()) is not None:
                    self.word_map.append((word.lower(), 1))

    def reducer(self, filename):
        print("Reducer: counting word frequency for tweets and articles ...")
        word_count_dictionary = {}

        for keyword in self.keywords:
            word_count_dictionary[keyword] = 0

        for word in self.word_map:
            key_value = self.isWordExists(word[0])
            word_count_dictionary[key_value] = word_count_dictionary[key_value] + 1
        self.save_data(filename, word_count_dictionary)

    def isWordExists(self, word):
        for keyword in self.keywords:
            if word.find(keyword) != -1:
                return keyword
        return None

    def save_data(self, filename, word_count_dictionary):
        print("Saving data ...")
        file_object = open(filename, "w+")
        file_object.write(str(word_count_dictionary))
        file_object.close()