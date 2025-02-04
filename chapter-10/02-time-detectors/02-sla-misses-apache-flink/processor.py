import json
from typing import Any

from elasticsearch import Elasticsearch
from pyflink.datastream import ProcessFunction

from models import ReducedVisitWrapper


class ReducedVisitWithProcessingTimeMetricProcessor(ProcessFunction):

    def process_element(self, value: ReducedVisitWrapper, ctx: 'ProcessFunction.Context'):
        yield json.dumps(value.to_dict())



class SlaDocumentsProcessor(ProcessFunction):

    ES_CLIENT = Elasticsearch('http://localhost:9200', maxsize=5, http_auth=('elastic', 'changeme'))

    def process_element(self, sla_dump: dict[str, Any], ctx: 'ProcessFunction.Context'):
        response = SlaDocumentsProcessor.ES_CLIENT.index(index='visits_sync_sla', document=sla_dump)
        print(f'Sending {sla_dump}')
        if '_id' not in response:
            print(f'Error while indexing {sla_dump}')
            return ['Error']
        return ['OK']