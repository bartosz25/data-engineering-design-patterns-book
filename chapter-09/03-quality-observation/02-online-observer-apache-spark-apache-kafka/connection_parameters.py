from elasticsearch import Elasticsearch


def get_elasticsearch_client() -> Elasticsearch:
    return Elasticsearch('http://localhost:9200', maxsize=5, http_auth=('elastic', 'changeme'))


def get_validated_users_index_name() -> str:
    return 'visits_observation_stats'
