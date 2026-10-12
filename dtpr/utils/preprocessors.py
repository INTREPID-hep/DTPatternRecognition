from ..base import Event
from .functions import deltaR
from typing import Optional, Any, Sequence, List


def test_preprocessor(event: Event, dummy_val: Optional[int] = -999) -> None:
    try:
        p1, p2 = event.genmuons
        event.dR = deltaR(p1, p2)
    except ValueError:
        event.dR = dummy_val


def resolve_collection_by_composite_key(
    event: Event,
    source_collection: str,
    target_collection: str,
    source_key_attrs: Sequence[str],
    target_key_attrs: Sequence[str],
    resultColName: str,
) -> List[Any]:
    """
    Resolve a flattened collection using a composite key.

    This is useful when a single identifier is not enough to uniquely match
    objects, for example when digis share the same wire but differ by layer.

    :param source_collection: Name of the source collection to resolve.
    :type source_collection: str
    :param target_collection: Name of the target collection to resolve against.
    :type target_collection: str
    :param source_key_attrs: Attribute names on the source object that define the key.
    :type source_key_attrs: Sequence[str]
    :param target_key_attrs: Attribute names on the target objects. If omitted, the
        source attribute names are reused.
    :type target_key_attrs: Optional[Sequence[str]]
    :return: List of matched target objects, preserving source order.
    :rtype: List[Any]
    """
    if target_key_attrs is None:
        target_key_attrs = source_key_attrs

    if len(source_key_attrs) != len(target_key_attrs):
        raise ValueError("source_key_attrs and target_key_attrs must have the same length")

    def _as_list(value: Any) -> List[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        return [value]

    for p_source in getattr(event, source_collection):

        source_values = [_as_list(getattr(p_source, attr, None)) for attr in source_key_attrs]
        if not source_values or any(not values for values in source_values):
            return []

        lookup = {}
        for target in getattr(event, target_collection):
            key = tuple(getattr(target, attr, None) for attr in target_key_attrs)
            if None in key:
                continue
            lookup.setdefault(key, target)

        resolved = []
        for key in zip(*source_values):
            resolved.append(lookup.get(tuple(key)))

        setattr(p_source, resultColName, resolved)
