import json
from typing import Dict, Any

from connection_parameters import get_validated_users_index_name, get_elasticsearch_client


def index_validation_results_to_elastichsearch(observations: Dict[str, Any]):
    es_client = get_elasticsearch_client()
    document_to_index = json.dumps(observations)
    print(document_to_index)
    response = es_client.index(index=get_validated_users_index_name(), document=document_to_index)
    if '_id' not in response:
        print(f'Error while indexing {document_to_index}')
