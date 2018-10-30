import argparse

from jsonschema import validate
import json
try:
    import yaml
except ImportError:
    yaml = None

# TODO: add smart validators:
# 1. hw_mgmt: validate minItems regarding type of hw_mgmt
# 2.
schema = {
    "type": "object",
    "properties": {
        "environment": {
            "type": "object",
            "properties": {
                "data_directors": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "username": {"type": "string"},
                            "password": {"type": "string"},
                            "address": {"type": "string"},
                            "hw_mgmt": {
                                "type": "array",
                                "items": {"type": "string"},
                                "minItems": 3,
                                "uniqueItems": False
                                }
                            },
                        "required": ["username", "password", "address"]
                        },
                    "minItems": 0,
                    "uniqueItems": True
                    },
                "data_stores": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "username": {"type": "string"},
                            "password": {"type": "string"},
                            "address": {"type": "string"},
                            "exports": {
                                "type": "array",
                                "items": {"type": "string"},
                                "minItems": 0,
                                "uniqueItems": True
                                },
                            "hw_mgmt": {
                                "type": "array",
                                "items": {"type": "string"},
                                "minItems": 3,
                                "uniqueItems": False
                                }
                            },
                        "required": ["username", "password", "address"]
                        },
                    "minItems": 0,
                    "uniqueItems": True
                    },
                "clients": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "username": {"type": "string"},
                            "password": {"type": "string"},
                            "address": {"type": "string"},
                            "hw_mgmt": {
                                "type": "array",
                                "items": {"type": "string"},
                                "minItems": 3,
                                "uniqueItems": False
                                }
                            },
                        "required": ["username", "password", "address"]
                        },
                    "minItems": 0,
                    "uniqueItems": True
                    },
                "servers": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "username": {"type": "string"},
                            "password": {"type": "string"},
                            "address": {"type": "string"},
                            "hw_mgmt": {
                                "type": "array",
                                "items": {"type": "string"},
                                "minItems": 3,
                                "uniqueItems": False
                                },
                            "role": {"type": "string"}
                            },
                        "required": ["username", "password", "address"]
                        },
                    "minItems": 0,
                    "uniqueItems": True
                    },
                "data_portals": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "username": {"type": "string"},
                            "password": {"type": "string"},
                            "address": {"type": "string"},
                            "hw_mgmt": {
                                "type": "array",
                                "items": {"type": "string"},
                                "minItems": 3,
                                "uniqueItems": False
                                },
                            "role": {"type": "string"}
                            },
                        "required": ["username", "password", "address"]
                        },
                    "minItems": 0,
                    "uniqueItems": True
                    }
                }
            },
        "drms": {
                 },
        "report": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "auth": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 2,
                    "maxItems": 2,
                    "uniqueItems": False
                    }
                }
            },
        "tests": {"type": "object"},
        "variables": {"type": "object"}
        }
    }


def set_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-j', '--json-file',
                        required=True,
                        action='store',
                        help='Path to system json file')
    return parser.parse_args()

def load_config_file(path):
    '''Load the config file, trying yaml if it is available'''
    with open(path) as f:
        data = f.read()
    try:
        return json.loads(data)
    except ValueError:
        if not yaml:
            raise
    return yaml.load(data)

def verify_json_file(json_file):
    data = load_config_file(json_file)
    validate(data, schema)
    return data

def cli():
    args = set_parser()
    verify_json_file(args.json_file)
