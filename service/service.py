#!/usr/bin/env python3
"""
REST service to fetch data from MISP appliance into Sesam integration platform
"""
import os
import json
import logging
import requests
from requests.auth import HTTPBasicAuth
from flask import Flask, Response, request

APP = Flask(__name__)

ENV = os.environ.get

URL = ENV("baseurl")
EXPAND_PROPERTY_NAME = ENV("expand_property_name")
ID_PROPERTY_NAME = ENV("id_property_name")
USERNAME = ENV("username")
PASSWORD = ENV("password")
PROJECT_KEY = ENV("project_key")
DATA_KEY = ENV("data_key")
LOG_LEVEL = ENV('LOG_LEVEL', "INFO")
PORT = int(ENV('PORT', '5000'))
CT = 'application/json'
MIPS_REQUEST_HEADERS = {'Content-Type': CT}


def expand_entity(entity):
    """
    Donload and add extended data to given entity
    :param entity input data entity:
    :return: entity with added expand data
    """
    id_property_name_value = entity[ID_PROPERTY_NAME]
    request_url = "{0}{1}".format(URL, id_property_name_value)

    logging.info("Downloading data from: '%s'", request_url)

    try:
        expand_result = requests.get(request_url, headers=MIPS_REQUEST_HEADERS, auth=HTTPBasicAuth(USERNAME, PASSWORD))
        entity[EXPAND_PROPERTY_NAME] = json.loads(expand_result.text)
    except Exception as exc:
        logging.warning("Exception occurred when download data from '%s': '%s'", request_url, exc)
        raise exc

    return entity


def get_entities_per_project(projects, path, args):
    """

    :param projects:
    :param path:
    :param args:
    :return:
    """
    deduplicated_project_list = []

    for project in projects[DATA_KEY]:
        if project[PROJECT_KEY] not in deduplicated_project_list:
            deduplicated_project_list.append(project[PROJECT_KEY])

    logging.info(f"Got {len(deduplicated_project_list)} unique projects")

    for project in deduplicated_project_list:
        new_path = URL + path + str(project)
        logging.info("Trying GET operation on : '%s'", new_path)
        try:
            response = requests.get(new_path, headers=MIPS_REQUEST_HEADERS, auth=HTTPBasicAuth(USERNAME, PASSWORD))
            response.raise_for_status()
            entities = json.loads(response.text)
            logging.info(f"Got {len(entities[DATA_KEY])} entities for project {project}")
            logging.debug(f'payload: {entities}')
            for entity in entities[DATA_KEY]:
                yield set_id(project, entity, args)

        except requests.exceptions.HTTPError as exc:
            logging.error("Exception occurred on GET operation on '%s': '%s'", new_path, exc)


def get(data):
    """

    :param data:
    :return:
    """
    for item in data[DATA_KEY]:
        yield item


def set_id(project_id, entity, args):
    """

    :param project_id:
    :param entity:
    :param args:
    :return:
    """
    entity["ProjectId"] = project_id
    entity["_id"] = str(project_id) + "-" + str(entity[args.get('id')])
    return entity


def stream_json(entity):
    """

    :param entity:
    :return:
    """
    first = True
    yield '['
    for _, row in enumerate(entity):
        if not first:
            yield ','
        else:
            first = False
        yield json.dumps(row)
    yield ']'


@APP.route('/transform', methods=['POST'])
def receiver():
    """
    HTTP transform POST handler
    :return:
    """

    def generate(entities):
        """

        :param entities:
        :return:
        """
        yield "["
        for index, entity in enumerate(entities):
            if index > 0:
                yield ","

            yield json.dumps(expand_entity(entity))
        yield "]"

    logging.info(f"baseurl: {URL}")
    logging.info(f"expand_property_name: {EXPAND_PROPERTY_NAME}")
    logging.info(f"id_property_name: {ID_PROPERTY_NAME}")

    req_entities = request.get_json()

    try:
        return Response(generate(req_entities), mimetype=CT)
    except BaseException as exc:
        logging.warning(f"Exception {exc} occurred during execution of receiver function")
        return Response(status=500, response="An error occurred during transform of input")


@APP.route('/<path:path>', methods=['POST'])
def put(path):
    """
    HTTP transform POST handler
    :return:
    """
    responses = []
    req_entities = request.get_json()
    for entity in req_entities:
        project = str(entity["project_id"])
        data = entity["data"]
        path = URL + path + project

        logging.info(f"url: {path}")

        # Generate PUT operation
        try:
            logging.info(f"trying post operation on id: {project}")
            response = requests.put(path, data=json.dumps(data), headers=MIPS_REQUEST_HEADERS,
                                    auth=HTTPBasicAuth(USERNAME, PASSWORD))
            response.raise_for_status()
            responses.append(dict({project: json.loads(response.text)}))
        except requests.exceptions.HTTPError as exc:
            logging.error("Exception occurred on PUT operation on '%s': '%s'", path, exc)
            return Response(status=response.status_code, response="An error occurred during transform of input")

    logging.info("responses : %s", json.dumps(responses))
    return Response(response=json.dumps(responses), mimetype=CT)


@APP.route("/<path:path>", methods=["GET"])
def get_single_entities(path):
    """

    :param path:
    :return:
    """
    projects_path = URL + os.environ.get("project_path")
    try:
        logging.info("Trying GET operation on : '%s'", projects_path)
        response = requests.get(projects_path, headers=MIPS_REQUEST_HEADERS, auth=HTTPBasicAuth(USERNAME, PASSWORD))
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        logging.error("Exception occurred on GET operation on '%s': '%s'", projects_path, exc)
        return Response(status=response.status_code, response="An error occurred during transform of input")

    return Response(response=stream_json(get_entities_per_project(
        json.loads(response.text), path, request.args)), mimetype=CT)


@APP.route("/get/<path:path>", methods=["GET"])
def get_projects(path):
    """

    :param path:
    :return:
    """
    path = URL + path
    try:
        logging.info("Trying GET operation on : '%s'", path)
        response = requests.get(path, headers=MIPS_REQUEST_HEADERS, auth=HTTPBasicAuth(USERNAME, PASSWORD))
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        logging.error("Exception occurred on GET operation on '%s': '%s'", path, exc)
        return Response(status=response.status_code, response="An error occurred during transform of input")

    return Response(response=stream_json(get(
        json.loads(response.text))), mimetype=CT)


if __name__ == '__main__':
    logging.basicConfig(level=logging.getLevelName(LOG_LEVEL))

    IS_DEBUG_ENABLED = True if logging.getLogger().isEnabledFor(logging.DEBUG) else False

    if IS_DEBUG_ENABLED:
        APP.run(debug=IS_DEBUG_ENABLED, host='0.0.0.0', port=PORT)
    else:
        import cherrypy

        cherrypy.tree.graft(APP, '/')
        cherrypy.config.update({
            'environment': 'production',
            'engine.autoreload_on': True,
            'log.screen': False,
            'server.socket_port': PORT,
            'server.socket_host': '0.0.0.0',
            'server.thread_pool': 10,
            'server.max_request_body_size': 0
        })

        cherrypy.engine.start()
        cherrypy.engine.block()
