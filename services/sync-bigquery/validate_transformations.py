#!/usr/bin/env python3
"""
Validation script for creation_transformations and runtime_transformations JSON data.
Implements the same validation logic as the PostgreSQL schema constraints.
"""

import json
from typing import Any, List, Dict, Optional


def validate_creation_transformations(transformations: Any) -> tuple[bool, Optional[str]]:
    """
    Validate creation_transformations JSON according to database schema.

    Returns:
        (is_valid, error_message)
    """
    if transformations is None:
        return True, None

    # Must be a list/array
    if not isinstance(transformations, list):
        return False, f"creation_transformations must be an array, got {type(transformations).__name__}"

    # Validate each transformation
    allowed_reasons = ['constructorArguments', 'library', 'cborAuxdata']

    for idx, transformation in enumerate(transformations):
        # Must be an object/dict
        if not isinstance(transformation, dict):
            return False, f"Transformation at index {idx} must be an object, got {type(transformation).__name__}"

        # Must have 'reason' key
        if 'reason' not in transformation:
            return False, f"Transformation at index {idx} missing required key 'reason'. Keys: {list(transformation.keys())}"

        # 'reason' must be a string
        reason = transformation['reason']
        if not isinstance(reason, str):
            return False, f"Transformation at index {idx} 'reason' must be a string, got {type(reason).__name__}"

        # 'reason' must be in allowed list
        if reason not in allowed_reasons:
            return False, f"Transformation at index {idx} has invalid reason '{reason}'. Allowed: {allowed_reasons}"

        # Validate based on reason type
        if reason == 'constructorArguments':
            is_valid, error = validate_constructor_arguments_transformation(transformation, idx)
            if not is_valid:
                return False, error
        elif reason == 'library':
            is_valid, error = validate_library_transformation(transformation, idx)
            if not is_valid:
                return False, error
        elif reason == 'cborAuxdata':
            is_valid, error = validate_cbor_auxdata_transformation(transformation, idx)
            if not is_valid:
                return False, error

        # Note: We don't check for extra keys because PostgreSQL validation doesn't enforce this
        # The database only validates that required keys exist with correct values

    return True, None


def get_expected_keys(reason: str) -> set:
    """Get expected keys for a transformation based on reason"""
    base_keys = {'reason', 'type', 'offset'}

    if reason == 'constructorArguments':
        return base_keys
    elif reason in ['library', 'cborAuxdata']:
        return base_keys | {'id'}

    return base_keys


def validate_constructor_arguments_transformation(transformation: dict, idx: int) -> tuple[bool, Optional[str]]:
    """Validate constructorArguments transformation"""
    # Must have 'type' key with value 'insert'
    if 'type' not in transformation:
        return False, f"Transformation at index {idx} (constructorArguments) missing required key 'type'"

    if transformation['type'] != 'insert':
        return False, f"Transformation at index {idx} (constructorArguments) must have type='insert', got '{transformation['type']}'"

    # Must have 'offset' key
    if 'offset' not in transformation:
        return False, f"Transformation at index {idx} (constructorArguments) missing required key 'offset'"

    # 'offset' must be a non-negative integer
    offset = transformation['offset']
    if not isinstance(offset, int) or offset < 0:
        return False, f"Transformation at index {idx} (constructorArguments) 'offset' must be a non-negative integer, got {offset} ({type(offset).__name__})"

    return True, None


def validate_library_transformation(transformation: dict, idx: int) -> tuple[bool, Optional[str]]:
    """Validate library transformation"""
    # Must have 'type' key with value 'replace'
    if 'type' not in transformation:
        return False, f"Transformation at index {idx} (library) missing required key 'type'"

    if transformation['type'] != 'replace':
        return False, f"Transformation at index {idx} (library) must have type='replace', got '{transformation['type']}'"

    # Must have 'offset' key
    if 'offset' not in transformation:
        return False, f"Transformation at index {idx} (library) missing required key 'offset'"

    # 'offset' must be a non-negative integer
    offset = transformation['offset']
    if not isinstance(offset, int) or offset < 0:
        return False, f"Transformation at index {idx} (library) 'offset' must be a non-negative integer, got {offset} ({type(offset).__name__})"

    # Must have 'id' key
    if 'id' not in transformation:
        return False, f"Transformation at index {idx} (library) missing required key 'id'"

    # 'id' must be a non-empty string
    id_val = transformation['id']
    if not isinstance(id_val, str) or len(id_val) == 0:
        return False, f"Transformation at index {idx} (library) 'id' must be a non-empty string, got {repr(id_val)} ({type(id_val).__name__})"

    return True, None


def validate_cbor_auxdata_transformation(transformation: dict, idx: int) -> tuple[bool, Optional[str]]:
    """Validate cborAuxdata transformation"""
    # Must have 'type' key with value 'replace'
    if 'type' not in transformation:
        return False, f"Transformation at index {idx} (cborAuxdata) missing required key 'type'"

    if transformation['type'] != 'replace':
        return False, f"Transformation at index {idx} (cborAuxdata) must have type='replace', got '{transformation['type']}'"

    # Must have 'offset' key
    if 'offset' not in transformation:
        return False, f"Transformation at index {idx} (cborAuxdata) missing required key 'offset'"

    # 'offset' must be a non-negative integer
    offset = transformation['offset']
    if not isinstance(offset, int) or offset < 0:
        return False, f"Transformation at index {idx} (cborAuxdata) 'offset' must be a non-negative integer, got {offset} ({type(offset).__name__})"

    # Must have 'id' key
    if 'id' not in transformation:
        return False, f"Transformation at index {idx} (cborAuxdata) missing required key 'id'"

    # 'id' must be a non-empty string
    id_val = transformation['id']
    if not isinstance(id_val, str) or len(id_val) == 0:
        return False, f"Transformation at index {idx} (cborAuxdata) 'id' must be a non-empty string, got {repr(id_val)} ({type(id_val).__name__})"

    return True, None


def validate_runtime_transformations(transformations: Any) -> tuple[bool, Optional[str]]:
    """
    Validate runtime_transformations JSON according to database schema.
    Runtime transformations can have additional 'immutable' and 'callProtection' reasons.

    Returns:
        (is_valid, error_message)
    """
    if transformations is None:
        return True, None

    # Must be a list/array
    if not isinstance(transformations, list):
        return False, f"runtime_transformations must be an array, got {type(transformations).__name__}"

    # Validate each transformation
    allowed_reasons = ['constructorArguments', 'library', 'cborAuxdata', 'immutable', 'callProtection']

    for idx, transformation in enumerate(transformations):
        # Must be an object/dict
        if not isinstance(transformation, dict):
            return False, f"Transformation at index {idx} must be an object, got {type(transformation).__name__}"

        # Must have 'reason' key
        if 'reason' not in transformation:
            return False, f"Transformation at index {idx} missing required key 'reason'. Keys: {list(transformation.keys())}"

        # 'reason' must be a string
        reason = transformation['reason']
        if not isinstance(reason, str):
            return False, f"Transformation at index {idx} 'reason' must be a string, got {type(reason).__name__}"

        # 'reason' must be in allowed list
        if reason not in allowed_reasons:
            return False, f"Transformation at index {idx} has invalid reason '{reason}'. Allowed: {allowed_reasons}"

        # Note: We're not validating immutable and callProtection here as they're not in creation_transformations
        # But the validation logic would be similar

    return True, None


if __name__ == '__main__':
    # Test with some examples
    valid_examples = [
        [],
        [{"reason": "constructorArguments", "type": "insert", "offset": 0}],
        [{"reason": "library", "type": "replace", "offset": 0, "id": "file1:lib1"}],
        [{"reason": "cborAuxdata", "type": "replace", "offset": 5646, "id": "1"}],
        [
            {"reason": "constructorArguments", "type": "insert", "offset": 0},
            {"reason": "library", "type": "replace", "offset": 100, "id": "file1:lib1"},
            {"reason": "cborAuxdata", "type": "replace", "offset": 5646, "id": "1"}
        ]
    ]

    invalid_examples = [
        # Wrong type (string instead of array)
        "not an array",
        # Missing 'reason'
        [{"type": "insert", "offset": 0}],
        # Invalid reason
        [{"reason": "invalid", "type": "insert", "offset": 0}],
        # Wrong type for constructorArguments
        [{"reason": "constructorArguments", "type": "replace", "offset": 0}],
        # Missing id for library
        [{"reason": "library", "type": "replace", "offset": 0}],
        # Negative offset
        [{"reason": "cborAuxdata", "type": "replace", "offset": -1, "id": "1"}],
    ]

    print("Testing valid examples:")
    for example in valid_examples:
        is_valid, error = validate_creation_transformations(example)
        print(f"  {example}: {'✓' if is_valid else '✗'} {error or ''}")

    print("\nTesting invalid examples:")
    for example in invalid_examples:
        is_valid, error = validate_creation_transformations(example)
        print(f"  {example}: {'✓' if is_valid else '✗'} {error or ''}")
