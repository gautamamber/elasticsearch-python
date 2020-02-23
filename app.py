from elasticsearch import Elasticsearch
from data_set import data_set


def match_search(elastic_object, index_name):
    """
    Perform match search, it will take body as a parameter which is json type
    In body: from = offset from current, size: total matched document, Query: what type of query and what key
    :param elastic_object: Instance of elastic search
    :param index_name: Index name
    :return:
    """
    data = elastic_object.search(index=index_name, body={'from': 0, 'size': 1, 'query': {'match': {'name': 'cake'}}})
    print("Searched data is", data)


def get_data_using_id(elastic_object, index_name, _id):
    """
    Get stored data using id
    :param elastic_object: Elastic search instance
    :param index_name: Index name
    :param id: id to get data
    :return:
    """
    data = elastic_object.get(index=index_name, doc_type='sample_records', id=_id)
    """
    Data is present in source key
    """
    print("Get data is", data['_source'])


def search_operations(elastic_object, index_name, stored_result):
    """
    Performing various search and get operations
    :param elastic_object: Instance of elastic search
    :param index_name: Index name
    :param stored_result: Stored result in index to perform get and search operations
    :return:
    """
    """
    Get data using Id
    """
    get_data_using_id(elastic_object, index_name, stored_result['_id'])
    """
    Search
    """
    match_search(elastic_object, index_name)


def create_index(elastic_object, index_name):
    """
    Create new index also similar to table
    :return:
    """
    created = False
    try:
        if not elastic_object.indices.exists(index_name):
            elastic_object.indices.create(index=index_name, ignore=400)
            print("Index created")
        created = True
    except Exception as ex:
        print("Exception is: ", str(ex))
    finally:
        return created


def store_record(elastic_object, index_name, example_data):
    """
    Store some sample data in Index
    After store we are performing various operations such as get, search, scan by calling respective methods
    :param elastic_object: elastic search object after connection
    :param index_name: Name of index inn which data is to be stored
    :param example_data: Some sample data
    :return:
    """
    is_stored = True
    try:
        for data in example_data:
            outcome = elastic_object.index(index=index_name, doc_type='sample_records', body=data)
            print("Data added successfully")
            """
            Perform various search operations
            """
            search_operations(elastic_object, index_name, outcome)
    except Exception as ex:
        print("Error in indexing Data", ex)
        is_stored = False
    finally:
        return is_stored


def connect_elastic_search():
    """
    Connect elastic search on localhost:9200
    :return:
    """
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
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
    example_data = data_set
    index_name = "example_index"
    es = connect_elastic_search()
    if es is not None:
        if create_index(es, index_name):
            print("Index Created successfully...")
            store = store_record(es, index_name, example_data)
            if store:
                print("Store record successfully")
    return "Hello world"


if __name__ == "__main__":
    print(main())
