#!/usr/bin/env python3
"""
REST service to fetch data from MISP appliance into Sesam integration platform
"""
import os
import logging
import requests
from requests.auth import HTTPBasicAuth
from flask import Flask, Response, request
import rapidjson

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
MIPS_REQUEST_HEADERS = {'Content-Type': CT, 'Accept': CT}

# these config params needed to fetch work order operations
# through a HTTP transformation from "workorder" pipe
TRANSFORM_PROJECT_ID = ENV('TRANSFORM_PROJECT_ID', 'mips-workorder:ProjectId')
TRANSFORM_WORKORDER_ID = ENV('TRANSFORM_WORKORDER_ID', 'mips-workorder:Id')


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
        entity[EXPAND_PROPERTY_NAME] = rapidjson.loads(expand_result.text)
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

    if not args.get('projects'):
        allowed_projects = ['all']
    else:
        allowed_projects = args.get('projects').split(',')

    logging.debug(f'allowed projects {allowed_projects}')
    logging.info(f"got {len(deduplicated_project_list)} unique projects")

    for project in deduplicated_project_list:
        if 'all' not in allowed_projects and str(project) not in allowed_projects:
            logging.debug(f'project {project} not in allowed projects and will be skipped')
            continue
        exc_flag = ""
        new_path = URL + path + str(project)
        logging.debug("trying GET operation on : '%s'", new_path)
        try:
            response = requests.get(new_path, headers=MIPS_REQUEST_HEADERS, auth=HTTPBasicAuth(USERNAME, PASSWORD))
            response.raise_for_status()
            logging.debug(f"request completed in {response.elapsed} seconds")
            entities = rapidjson.loads(response.text)

            logging.debug(f"got {len(entities[DATA_KEY])} entities for project {project}")
            if len(entities[DATA_KEY]) > 0:
                logging.debug(f'payload (1 element): {entities[DATA_KEY][0]}')

            for entity in entities[DATA_KEY]:
                entity["ProjectId"] = project
                yield set_id(project, entity, args)
        except requests.exceptions.HTTPError as exc:
            logging.error("exception occurred on GET operation on '%s': '%s'", new_path, exc)
            exc_flag = "un"
            if response.text:
                logging.error(f'Response: {response.text}')
            if response.status_code >= 500:
                raise exc

        logging.debug(f"project {str(project)} executed {exc_flag}successfully")


def get(item):
    """
    Function iterates over items in DATA_KEY property of given item
    Will throw KetError if DATA_KEY doesn't exist
    :param item: dictionary which MUST contain DATA_KEY attribute of <class 'list'> type
    :return: yields items from DATA_KEY property
    """
    for item in item[DATA_KEY]:
        yield item


def set_id(project_id, entity, args):
    """

    :param project_id:
    :param entity:
    :param args:
    :return:
    """
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
        yield rapidjson.dumps(row)
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

            yield rapidjson.dumps(expand_entity(entity))
        yield "]"

    logging.info(f"baseurl: {URL}")
    logging.info(f"expand_property_name: {EXPAND_PROPERTY_NAME}")
    logging.info(f"id_property_name: {ID_PROPERTY_NAME}")

    req_entities = request.get_json()

    try:
        return Response(generate(req_entities), mimetype=CT)
    except BaseException as exc:
        logging.warning(f"exception {exc} occurred during execution of receiver function")
        return Response(status=500, response="An error occurred during transform of input")


@APP.route('/<path:path>', methods=['POST'])
def put(path):
    """
    HTTP transform POST handler
    :param path: must have trailing slash
    :return:
    """
    req_entities = request.get_json()
    mips_auth = HTTPBasicAuth(USERNAME, PASSWORD)
    for entity in req_entities:

        project = entity.get("project_id")

        if not project:
            raise ValueError("project_id must be presented in input entity")
        operation = entity["operation"].lower()
        response_entity = []
        response_entity["_id"] = entity["_id"]
        data = entity["data"]
        path = URL + path + str(project)

        try:
            logging.info(f"trying post operation to: {path}")
            if operation == "post":
                response = requests.post(path, data=rapidjson.dumps(data), headers=MIPS_REQUEST_HEADERS, auth=mips_auth)
            if operation == "put":
                response = requests.put(path, data=rapidjson.dumps(data), headers=MIPS_REQUEST_HEADERS, auth=mips_auth)
            response.raise_for_status()
            entity['transfer_status'] = rapidjson.loads(response.text)
            response_entity['response'] = rapidjson.loads(response.text)
        except requests.exceptions.HTTPError as exc:
            logging.error(f"exception '{exc}' occurred on POST operation on '{path}'")
            if response.text:
                logging.error(f"error message: {response.text}")
                logging.error(f"input entity: {entity}")
            return Response(status=response.status_code, response="An error occurred during transform of input")

    return Response(response=rapidjson.dumps(response_entity), mimetype=CT)


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
        rapidjson.loads(response.text), path, request.args)), mimetype=CT)


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
        rapidjson.loads(response.text))), mimetype=CT)


@APP.route('/workorderoperation/<int:order_id>/<int:project_id>', methods=["GET"])
def get_workorder_operation(order_id: int, project_id: int):
    """
    Endpoint to retrieve work order operations for given order and project
    :param order_id: id of order
    :param project_id: id of project
    :return: list of order operations
    """
    path = f'{URL}construction/v1/WorkOrderOperation/WorkOrder/{order_id}/null/null/null/0/null/0/0/{project_id}'
    try:
        logging.debug(f"Trying GET operation on : {path}")
        response = requests.get(path, headers=MIPS_REQUEST_HEADERS, auth=HTTPBasicAuth(USERNAME, PASSWORD))
        response.raise_for_status()
    except IOError as exc:
        logging.error(f"Exception occurred on GET operation on {path}: {exc}")
        return Response(status=response.status_code, response="An error occurred during transform of input")

    return Response(response=stream_json(get(rapidjson.loads(response.text))), mimetype=CT)


@APP.route('/workorderoperation', methods=['POST'])
def get_workorder_operations2():
    """
    This endpoint is intended to be used as a HTTP transform from Sesam appliance
    and similar to get_workorder_operation
    :return:
    """
    input_items = request.get_json()

    def enrich_with_workorder_operations(item_list):
        yield '['
        first = True
        for item in item_list:
            if not first:
                yield ','
            else:
                first = False
            logging.debug(item)
            project_id = item[TRANSFORM_PROJECT_ID]
            order_id = item[TRANSFORM_WORKORDER_ID]
            # we call get_workorder_operation endpoint on the same service to get details of work order
            wo_operations = requests.get(f'http://localhost:5000/workorderoperation/{order_id}/{project_id}')
            wo_operations.raise_for_status()
            item['work_order_operations'] = rapidjson.loads(wo_operations.text)
            yield rapidjson.dumps(item)
        yield ']'

    return Response(response=enrich_with_workorder_operations(input_items), mimetype=CT)


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
            'server.thread_pool': 32,
            'server.max_request_body_size': 0
        })

        cherrypy.engine.start()
        cherrypy.engine.block()
