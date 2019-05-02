import json
import pprint
import ijson.backends.yajl2_cffi as ijson
import datetime
from datetime import datetime as cl_datetime
from collections.abc import Mapping, Iterable
from base64 import b64encode, b64decode
from decimal import Decimal
from uuid import UUID

JSON_ENCODER_LAMBDAS = {
    bytes: lambda o: "~b%s" % str(b64encode(o), encoding="utf-8"),
    Decimal: lambda o: "~f%s" % o,
    UUID: lambda o: "~u%s" % str(o),

    # NOTE: these are types that do not come out from entity bytes
    datetime.date: lambda o: "~t%s" % '%04d' % o.year + o.strftime("-%m-%d"),
    datetime.datetime: lambda o: "~t%s" % '%04d' % o.year + o.strftime("-%m-%dT%H:%M:%S.%fZ")
}


def _entity_json_encoder(o):
    """JSON encoder function. Usage: json.dumps(entity, default=_entity_json_encoder)"""
    value_type = type(o)
    json_encoder = JSON_ENCODER_LAMBDAS.get(value_type)
    if json_encoder is None:
        if isinstance(o, Mapping):
            return dict(o)
        elif not isinstance(o, str) and isinstance(o, Iterable):
            return list(o)
        else:
            raise TypeError(repr(o) + " is not JSON serializable")
    else:
        return json_encoder(o)


def entities_to_json(entities, sort_keys=False, indent=None, cls=None):
    """Returns a JSON serialized string from the given entities. This method is able
    to properly serialize non-native JSON types supported in entity bytes."""
    return json.dumps(entities, default=_entity_json_encoder,
                      sort_keys=sort_keys, indent=indent, cls=cls)


def datetime_parse(dt_str):
    if len(dt_str) <= 27:  # len('2015-11-24T07:58:53.123456Z') == 27
        if len(dt_str) == 10:  # len('2015-11-24') == 10
            dt = cl_datetime.strptime(dt_str, "%Y-%m-%d")
            return dt
        elif len(dt_str) <= 20:  # len('2015-11-24T07:58:53Z') == 27
            dt = cl_datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")
            return dt
        else:
            dt = cl_datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ")
            return dt
    elif len(dt_str) > 30:  # len('2015-07-28T09:46:00.123456789Z') == 30
        raise Exception("Invalid date string: %s" % dt_str)
    else:
        dt = cl_datetime.strptime(dt_str[:19], "%Y-%m-%dT%H:%M:%S")  # len('2015-11-24T07:58:53') == 19
        return dt


def parse_json_stream(stream):
    """Parses stream of JSON entities. Stream can contain either a single
    JSON object or a list of JSON objects.
    :param file-like stream object
    :return: a generator of entity objects
    """
    entity_index = 0
    context = []
    name_context = []
    for event, value in ijson.basic_parse(stream):
        if event == 'start_map':
            context.append({})
        elif event == 'map_key':
            name_context.append(value)
        elif event == 'string':
            ctxobj = context[-1]
            if type(ctxobj) is list:
                if len(value) > 1 and value[0] == "~":
                    value1 = value[1]
                    if value1 == "r":
                        ctxobj.append(value)
                    elif value1 == "t":
                        ctxobj.append(datetime_parse(value[2:]))
                    elif value1 == "b":
                        ctxobj.append(b64decode(value[2:]))
                    elif value1 == "u":
                        ctxobj.append(UUID(value[2:]))
                    elif value1 == "f":
                        ctxobj.append(Decimal(value[2:]))
                    elif value1 == "~":
                        ctxobj.append(value[1:])
                    else:
                        ctxobj.append(value)
                else:
                    ctxobj.append(value)
            elif type(ctxobj) is dict:
                prop_name = name_context.pop()
                if len(value) > 1 and value[0] == "~":
                    value1 = value[1]
                    if value1 == "r":
                        ctxobj[prop_name] = value
                    elif value1 == "t":
                        ctxobj[prop_name] = datetime_parse(value[2:])
                    elif value1 == "b":
                        ctxobj[prop_name] = b64decode(value[2:])
                    elif value1 == "u":
                        ctxobj[prop_name] = UUID(value[2:])
                    elif value1 == "f":
                        ctxobj[prop_name] = Decimal(value[2:])
                    elif value1 == "~":
                        ctxobj[prop_name] = value[1:]
                    else:
                        ctxobj[prop_name] = value

                else:
                    ctxobj[prop_name] = value
            else:
                raise Exception("WAT!")
        elif event in {'number', 'boolean', 'null'}:
            ctxobj = context[-1]
            if type(ctxobj) is list:
                ctxobj.append(value)
            elif type(ctxobj) is dict:
                prop_name = name_context.pop()
                ctxobj[prop_name] = value
            else:
                raise Exception("WAT!")
        elif event == 'end_map':
            entity = context.pop()
            if len(context) == 1 and type(context[0]) is list or len(context) == 0:  # allow reading a single entity
                yield entity
                entity_index += 1
            else:
                parent = context[-1]
                if type(parent) is dict:
                    parent[name_context.pop()] = entity
                elif type(parent) is list:
                    parent.append(entity)
                else:
                    raise Exception("WAT!")
        elif event == 'start_array':
            context.append([])
        elif event == 'end_array':
            l = context.pop()
            if len(context) > 0:
                parent = context[-1]
                if type(parent) is list:
                    parent.append(l)
                else:
                    parent[name_context.pop()] = l

