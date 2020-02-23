from elasticsearch import Elasticsearch


def connect_elastic_search():
    """
    Connect elastic search on localhost:9200
    :return:
    """
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    _es
    if _es.ping():
        print("====connected====")
    else:
        print("Not connect")
    return _es


def main():
    """
    Entry point of elastic search, start
    1. Connect
    2. Create index
    3. Add document
    4. Insert
    5. Get Query
    6. Different search queries and matching
    7. Combining queries
    8. Regular expression
    9. Bulk insert
    10. Scan
    11. Analyzers
    :return:
    """
    es = connect_elastic_search()
    return "Hello world"


if __name__ == "__main__":
    print(main())
