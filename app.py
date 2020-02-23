from elasticsearch import Elasticsearch
from data_set import data_set


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
    :param elastic_object: elastic search object after connection
    :param index_name: Name of index inn which data is to be stored
    :param example_data: Some sample data
    :return:
    """
    is_stored = True
    try:
        for data in example_data:
            outcome = elastic_object.index(index=index_name, doc_type='sample_records', body=data)
            print("Data addedd successfully")
            """
            Get data using Id
            """
            get_data_using_id(elastic_object, index_name, outcome['_id'])
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
