#!/usr/bin/env python3
"""
REST service to fetch data from MISP appliance into Sesam integration platform
"""
import os
import io
import logging
import tempfile

import requests
from requests.auth import HTTPBasicAuth
from flask import Flask, Response, request, send_file, abort
from sesamutils.flask import serve
import rapidjson
import base64

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
WORKERS = int(ENV("MS_WORKER_THREADS", '32'))
CT = 'application/json'
MIPS_REQUEST_HEADERS = {'Content-Type': CT, 'Accept': CT}

# these config params needed to fetch work order operations
# through a HTTP transformation from "workorder" pipe

# where to take input project id from
TRANSFORM_PROJECT_ID = ENV('TRANSFORM_PROJECT_ID', 'mips-workorder:ProjectId')
# where to take input workorder id from
TRANSFORM_WORKORDER_ID = ENV('TRANSFORM_WORKORDER_ID', 'mips-workorder:Id')
# where to place fetched workorder operations
TRANSFORM_WO_OP_ATTR = ENV('TRANSFORM_WO_OP_ATTR', 'work_order_operations')

# if this key is presented as query argument in a source pipe then
# property named ProjectNo with corresponding value will be added to result entity
ADD_PROJECT_NO = ENV('ADD_PROJECT_NO', 'add_project_no')


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
    logging.debug(f"got {len(deduplicated_project_list)} unique projects")

    for project in deduplicated_project_list:

        if args.get(ADD_PROJECT_NO):
            project_no = next(item for item in projects[DATA_KEY] if item[PROJECT_KEY] == project).get('ProjectNo')

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

                if args.get(ADD_PROJECT_NO) and entity.get("ProjectNo"):
                    logging.warning(f'ProjectNo property already exixts')

                if args.get(ADD_PROJECT_NO):
                    entity["ProjectNo"] = project_no

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
        data = entity["data"]
        path = URL + path + str(project)

        try:
            logging.info(f"trying post operation to: {path}")
            if operation == "post":
                response = requests.post(path, data=rapidjson.dumps(data), headers=MIPS_REQUEST_HEADERS, auth=mips_auth)
            if operation == "put":
                response = requests.put(path, data=rapidjson.dumps(data), headers=MIPS_REQUEST_HEADERS, auth=mips_auth)
            response.raise_for_status()
            entity['transfer_status'] = 'SUCCESS'
            entity['transfer_message'] = rapidjson.loads(response.text)
        except requests.exceptions.HTTPError as exc:
            logging.error(f"exception '{exc}' occurred on POST operation on '{path}'")
            if response.text:
                entity['transfer_message'] = rapidjson.loads(response.text)
                logging.error(f"error message: {response.text}")
                logging.error(f"input entity: {entity}")
            entity['transfer_status'] = 'FAILED'

        # return Response(status=response.status_code,
        #                response=f"An error occurred during transform of input due to {response.text}")

    return Response(response=rapidjson.dumps(req_entities), mimetype=CT)


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
        logging.debug("Trying GET operation on : '%s'", path)
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


@APP.route('/deletepunch', methods=['POST'])
def delete_punch():
    """
        HTTP transform POST handler for pipe punch-delete-mips-endpoint
        :return:
        """
    req_entities = request.get_json()
    mips_auth = HTTPBasicAuth(USERNAME, PASSWORD)
    for entity in req_entities:

        project = entity.get("project_id")
        punch_id = entity.get('punchid')

        if not project:
            raise ValueError("project_id must be presented in input entity")
        operation = entity["operation"].lower()

        path = f'{URL}completion/v1/commpkg/CheckSheet/Item/Punch/Delete/{punch_id}/{project}'

        try:
            logging.info(f"trying delete operation to: {path}")
            if operation == "delete":
                response = requests.delete(path, headers=MIPS_REQUEST_HEADERS, auth=mips_auth)
            response.raise_for_status()
            entity['transfer_status'] = 'SUCCESS'
            entity['transfer_message'] = rapidjson.loads(response.text)
        except requests.exceptions.HTTPError as exc:
            logging.error(f"exception '{exc}' occurred on DELETE operation on '{path}'")
            if response.text:
                entity['transfer_message'] = rapidjson.loads(response.text)
                logging.error(f"error message: {response.text}")
                logging.error(f"input entity: {entity}")
            entity['transfer_status'] = 'FAILED'

        # return Response(status=response.status_code,
        #                response=f"An error occurred during transform of input due to {response.text}")

    return Response(response=rapidjson.dumps(req_entities), mimetype=CT)

@APP.route('/workorderoperation', methods=['POST'])
def get_workorder_operations2():
    """
    This endpoint is intended to be used as a HTTP transform from Sesam appliance
    and similar to get_workorder_operation
    :return:
    """
    input_items = request.get_json()
    logging.debug(f'processing batch of {len(input_items)} entities')

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
            logging.debug(f'requesting workorder operations for order {order_id}, project {project_id}')
            wo_operations = requests.get(f'http://localhost:5000/workorderoperation/{order_id}/{project_id}')
            wo_operations.raise_for_status()

            item['work_order_operations'] = rapidjson.loads(wo_operations.text)
            woop_len = len(item[TRANSFORM_WO_OP_ATTR])
            logging.debug(f'request completed for order {order_id}, project {project_id}, got {woop_len} lines')

            yield rapidjson.dumps(item)
        yield ']'

    return Response(response=enrich_with_workorder_operations(input_items), mimetype=CT)


@APP.route("/file/<path:path>", methods=["GET"])
def get_file(path):
    """
    Endpoint to receive files from MIPS
    :param path:
    :return: file stream
    """
    file_request_path = URL + path
    logging.debug(f'serving request to {file_request_path}')
    response = requests.get(file_request_path, stream=True, headers=MIPS_REQUEST_HEADERS,
                            auth=HTTPBasicAuth(USERNAME, PASSWORD))

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        logging.error(f'{response.text} : {exc}' if response.text else f'{exc}')
        raise exc

    # if we have json response with base64 encoded file content
    response_data = None
    if response.headers['Content-Type'].startswith('application/json'):
        logging.debug(f'got JSON response')
        response_data = rapidjson.loads(response.text)
        logging.debug(f'response entity received: {response_data}')

        if response_data.get('Data') and response_data.get('Data').get('Contents'):
            decoded_file_byte = base64.standard_b64decode(response_data.get('Data').get('Contents'))
            logging.debug(f'file length: {len(decoded_file_byte)}')
            return send_file(io.BytesIO(decoded_file_byte), mimetype='application/octet-stream')

    # if we have PDF file back
    if response.headers['Content-Type'] == 'application/pdf':

        logging.debug(f'got PDF response')
        fp = tempfile.TemporaryFile()
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                fp.write(chunk)
        fp.flush()

        def stream_and_remove_file():
            yield from fp
            fp.close()

        return Response(stream_and_remove_file(), mimetype='application/pdf')

    error = f"couldn't process MIPS response, response headers: {response.headers}"
    logging.warning(error)
    abort(500, error)


if __name__ == '__main__':
    logging.basicConfig(level=logging.getLevelName(LOG_LEVEL))

    IS_DEBUG_ENABLED = True if logging.getLogger().isEnabledFor(logging.DEBUG) else False

    if IS_DEBUG_ENABLED:
        APP.run(debug=IS_DEBUG_ENABLED, host='0.0.0.0', port=PORT)
    else:
        serve(APP, port=PORT, config={'server.thread_pool': WORKERS, 'server.max_request_body_size': 0})
