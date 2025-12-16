#!flask/bin/python

from flask import Flask

from webservice.endpoint_data_ingestion_fallback import create_data_ingestion_with_fallback_storage

app = Flask(__name__.split('.')[0])

if __name__ == '__main__':
    create_data_ingestion_with_fallback_storage(app)

    app.run(debug=True, port=8080)
