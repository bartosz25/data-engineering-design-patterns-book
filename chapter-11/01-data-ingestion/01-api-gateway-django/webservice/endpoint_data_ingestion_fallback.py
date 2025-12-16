import json
import os
import pathlib
import time
from typing import List, Tuple

from flask import request, Response

from webservice import kafka
from webservice.kafka import CallbackDataHolder, logger


def create_data_ingestion_with_fallback_storage(app):
    kafka_producer = kafka.create_producer()

    @app.route('/ingestion/ingest/<string:topic>/<string:event_key>', methods=['POST'])
    def new_data_with_fallback(topic: str, event_key: str):
        json_data_to_deliver = json.loads(request.data)
        referrer = request.headers['Referer']
        headers = [
            ('source', b'api'),
            ('referral', referrer.encode('utf-8'))
        ]
        callback_data_holder = CallbackDataHolder(event_key, json_data_to_deliver)
        for message_to_deliver in json_data_to_deliver:
            kafka_producer.produce(
                topic=topic,
                key=event_key.encode('utf-8'),
                value=json.dumps(message_to_deliver).encode('utf-8'),
                headers=headers,
                on_delivery=kafka.create_delivery_callback_function(callback_data_holder))

        kafka_producer.flush(timeout=20)

        failed_deliveries = callback_data_holder.get_failed_deliveries()
        is_successful_delivery = True if not failed_deliveries else False
        if not is_successful_delivery:
            file_name = f'data_{round(time.time()*1000)}'
            topic_path = f'/tmp/dedp/ch11/01-data-ingestion/01-api-gateway-django/fallback/topic={topic}'
            os.makedirs(topic_path, exist_ok=True)
            def build_failed_delivery(event: dict[str, str]) -> str:
                return json.dumps({'key': event['key'], 'value': event['payload'], 'referral': referrer})
            (pathlib.Path(f'{topic_path}/{file_name}')
                .write_text('\n'.join([build_failed_delivery(event) for event in failed_deliveries])))
            return Response(json.dumps({'success': is_successful_delivery, 'failed': failed_deliveries}),
                            status=200, mimetype='application/json')
        else:
            return Response(json.dumps({'success': True, 'failed': []}),
                            status=200, mimetype='application/json')
