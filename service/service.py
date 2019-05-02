import json
import requests
from flask import Flask, Response
import os
import logger
import cherrypy
from utils import parse_json_stream, entities_to_json

app = Flask(__name__)
logger = logger.Logger('mips-userprojects-service')
url = os.environ.get("baseurl")
target_id_from_source = os.environ.get("target_id_from_source")
target_id_value_from_source = os.environ.get("target_id_value_from_source")

@app.route("/<path:path>", methods=["GET"])
def get(path):
    request_url = "{0}{1}".format(url, path)
    headers = {'Content-Type': 'application/json', 'sap-id': path }

    logger.info("Downloading data from: '%s'", request_url)

    try:
        response = requests.get(request_url, headers=headers)
    except Exception as e:
        logger.warn("Exception occurred when download data from '%s': '%s'", request_url, e)
        raise

    return Response(response=response.text, mimetype='application/json')


def expand_entity(entity):
    target_id_value = entity[target_id_value_from_source]
    request_url = "{0}{1}".format(url, target_id_value)
    headers = {'Content-Type': 'application/json', target_id_from_source: target_id_value }

    logger.info("Downloading data from: '%s'", request_url)

    try:
        project_setup = requests.get(request_url, headers=headers)
    except Exception as e:
        logger.warn("Exception occurred when download data from '%s': '%s'", request_url, e)
        raise

    entity["projectsetup"] = project_setup
    return entity


@app.route('/transform', methods=['POST'])
def receiver():
    """ HTTP transform POST handler """

    def generate(entities):
        yield "["
        for index, entity in enumerate(entities):
            if index > 0:
                yield ","

            entity = expand_entity(entity)
            yield entities_to_json(entity)
        yield "]"

    # get entities from request
    req_entities = parse_json_stream(requests.stream)

    # Generate the response
    try:
        return Response(generate(req_entities), mimetype='application/json')
    except BaseException as e:
        return Response(status=500, response="An error occurred during transform of input")


if __name__ == '__main__':
    cherrypy.tree.graft(app, '/')

    # Set the configuration of the web server to production mode
    cherrypy.config.update({
        'environment': 'production',
        'engine.autoreload_on': False,
        'log.screen': True,
        'server.socket_port': 5001,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()
