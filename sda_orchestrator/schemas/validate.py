"""Validate JSON module with Draft7Validator."""

import json
from jsonschema import Draft7Validator, validators

from typing import Any, Dict, Generator
from pathlib import Path
from ..utils.logger import LOG


def load_schema(name: str) -> Dict:
    """Load JSON schemas."""
    module_path = Path(__file__).resolve().parent
    path = module_path.joinpath(f"{name}.json")

    if path.is_file():
        with open(str(path), "r") as fp:
            data = fp.read()

        return json.loads(data)
    else:
        LOG.error(f"Schema file {name} not found.")
        raise FileNotFoundError


def extend_with_default(validator_class: Draft7Validator) -> Draft7Validator:
    """Include default values present in JSON Schema."""
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator: Draft7Validator, properties: Dict, instance: Any, schema: str) -> Generator:
        """Set defaults in validator."""
        for property, subschema in properties.items():
            if "default" in subschema:
                instance.setdefault(property, subschema["default"])

        for error in validate_properties(
            validator,
            properties,
            instance,
            schema,
        ):
            # Difficult to unit test
            yield error  # pragma: no cover

    return validators.extend(
        validator_class,
        {"properties": set_defaults},
    )


ValidateJSON = extend_with_default(Draft7Validator)
