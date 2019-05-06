import json
import requests
from flask import Flask, Response, request
import os
import logger
import cherrypy
from requests.auth import HTTPBasicAuth

app = Flask(__name__)
logger = logger.Logger('mips-userprojects-service')
url = os.environ.get("baseurl")
target_id_from_source = os.environ.get("target_id_from_source")
target_id_value_from_source = os.environ.get("target_id_value_from_source")
username = os.environ.get("mips_username")
password = os.environ.get("mips_password")


@app.route("/<path:path>", methods=["GET"])
def get(path):
    def generate(entities):
        yield "["
        for index, entity in enumerate(entities):
            if index > 0:
                yield ","
            logger.info(str(index) + ": " + json.dumps(entity))
            yield json.dumps(entity)
        yield "]"

    request_url = "{0}{1}".format(url, path)
    headers = {'Content-Type': 'application/json'}

    logger.info("Downloading data from: '%s'", request_url)

    try:
        response = requests.get(request_url, headers=headers, auth=HTTPBasicAuth(username, password))
        #logger.info("Response = " + response.text)

    except Exception as e:
        logger.warn("Exception occurred when download data from '%s': '%s'", request_url, e)
        raise

    return Response(response=generate(json.dumps(response.text)), mimetype='application/json')


def expand_entity(entity):
    target_id_value = entity[target_id_value_from_source]
    request_url = "{0}{1}".format(url, target_id_value)
    headers = {'Content-Type': 'application/json'}

    logger.info("Downloading data from: '%s'", request_url)

    try:
        userproject = requests.get(request_url, headers=headers, auth=HTTPBasicAuth(username, password))
    except Exception as e:
        logger.warn("Exception occurred when download data from '%s': '%s'", request_url, e)
        raise

    return userproject


@app.route('/transform', methods=['POST'])
def receiver():
    """ HTTP transform POST handler """

    def generate(entities):
        yield "["
        for index, entity in enumerate(entities):
            if index > 0:
                yield ","

            yield expand_entity(json.dumps(entity))
        yield "]"

    # get entities from request
    req_entities = json.dumps(request.get_json())

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
