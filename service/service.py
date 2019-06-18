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
expand_property_name = os.environ.get("expand_property_name")
id_property_name = os.environ.get("id_property_name")
username = os.environ.get("username")
password = os.environ.get("password")


@app.route("/<path:path>", methods=["GET"])
def get(path):
    def generate(entities):
        yield "["
        yield json.dumps(entities)
        yield "]"

    request_url = "{0}{1}".format(url, path)
    headers = {'Content-Type': 'application/json'}

    logger.info("Downloading data from: '%s'", request_url)

    try:
        response = requests.get(request_url, headers=headers, auth=HTTPBasicAuth(username, password))
    except Exception as e:
        logger.warn("Exception occurred when download data from '%s': '%s'", request_url, e)
        raise

    return Response(response=generate(json.loads(response.text)), mimetype='application/json')


def expand_entity(entity):
    id_property_name_value = entity[id_property_name]
    request_url = "{0}{1}".format(url, id_property_name_value)
    headers = {'Content-Type': 'application/json'}

    logger.info("Downloading data from: '%s'", request_url)

    try:
        expand_result = requests.get(request_url, headers=headers, auth=HTTPBasicAuth(username, password))
        entity[expand_property_name] = json.loads(expand_result.text)
    except Exception as e:
        logger.warn("Exception occurred when download data from '%s': '%s'", request_url, e)
        raise

    return entity


@app.route('/transform', methods=['POST'])
def receiver():
    """ HTTP transform POST handler """

    def generate(entities):
        yield "["
        for index, entity in enumerate(entities):
            if index > 0:
                yield ","

            yield json.dumps(expand_entity(entity))
        yield "]"

    logger.info("baseurl: " + url)
    logger.info("expand_property_name: " + expand_property_name)
    logger.info("id_property_name: " + id_property_name)

    # get entities from request
    req_entities = request.get_json()

    # Generate the response
    try:
        return Response(generate(req_entities), mimetype='application/json')
    except BaseException as e:
        return Response(status=500, response="An error occurred during transform of input")


@app.route('/<path:path>', methods=['POST'])
def put(path):
    """ HTTP transform POST handler """

    headers = {'Content-Type': 'application/json'}
    responses = []
    req_entities = request.get_json()
    for entity in req_entities:
        project = str(entity["project_id"])
        data = entity["data"]
        path = url + path + project

        logger.info("url: " + path)

        # Generate PUT operation
        try:
            logger.info("trying post operation on id: " + project)
            response = requests.put(path, data=json.dumps(data), headers=headers, auth=HTTPBasicAuth(username, password))
            responses.append(dict({project: json.loads(response.text)}))
            if response.status_code != 200:
                return Response(status=response.status_code, response=response.text)
        except Exception as e:
            logger.error("Exception occurred on PUT operation on '%s': '%s'", path, e)
            return Response(status=response.status_code, response="An error occurred during transform of input")

    logger.info("responses : %s", json.dumps(responses))
    return Response(response=json.dumps(responses), mimetype='application/json')


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
