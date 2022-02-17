from opensearchpy import OpenSearch
import json

host = 'localhost'
port = 9276
auth = ('admin', 'admin')
index_name = 'tweets'

client = OpenSearch(
    hosts = [{'host': host, 'port': port}],
    http_compress = True, # enables gzip compression for request bodies
    http_auth = auth,
    # client_cert = client_cert_path,
    # client_key = client_key_path,
    use_ssl = True,
    verify_certs = False,
    ssl_assert_hostname = False,
    ssl_show_warn = False,
    # ca_certs = ca_certs_path
)

def map_to_bulk_request(docs):
    request=[]
    for doc in docs:
        action = {
            'index': {
                '_index': index_name,
                '_id': doc['id']
            }
        }
        request.append(f'{json.dumps(action)}')
        request.append(doc)
    print(request)
    return request

def save_to_opensearch(docs):
    response = client.bulk(
        index=index_name,
        body=map_to_bulk_request(docs),
        refresh=True
    )

    print('\nAdding document:')
    print(response)

    if response['errors']:
        raise RuntimeError('Unable to index to Opensearch!')
