from ..utils.functions import color_msg, get_callable_from_src
from .entry_view import materialize
from functools import partial
from collections.abc import Iterable


_EXPRESSION_CACHE = {}


def compile_expression(text):
    """Compile an expression once per distinct text.

    The same expressions are evaluated for every particle of every event - the
    ``expr`` attributes, the ``filter`` and the ``sorter`` key of each particle type.
    Compiling them again for every particle is pure parser work: an
    ``eval("text")`` costs about 9 us, 50 times an ``eval(code_object)``.

    :param text: The expression to compile.
    :type text: str
    :returns: The compiled code object.
    :rtype: code
    :raises SyntaxError: If the expression is not valid Python.
    """
    code = _EXPRESSION_CACHE.get(text)
    if code is None:
        code = _EXPRESSION_CACHE[text] = compile(text, "<string>", "eval")
    return code


def copy_literal(value):
    """Return a fresh copy of a literal attribute.

    Literal attributes come from the config (``matched_tps: []``, ``showered:
    False``), so one object would otherwise be shared by every particle that
    uses the same key. Only plain containers are copied: deepcopy() costs 5x as
    much and literals are scalars or lists of scalars.
    """
    if isinstance(value, list):
        return [copy_literal(item) for item in value]
    if isinstance(value, dict):
        return {key: copy_literal(item) for key, item in value.items()}
    if isinstance(value, set):
        return set(value)
    return value


def flat_pair_bounds(event, counts_branch, counts, index):
    """Start and end of particle ``index`` inside a ``[flat, counts]`` branch pair.

    Uses the per-entry cumulative offsets of an :class:``EntryView`` when the
    event provides them, and falls back to summing the counts otherwise.

    :param event: The event entry the attribute is read from.
    :param counts_branch: Name of the branch holding the per-particle counts.
    :type counts_branch: str
    :param counts: The materialized counts of the branch.
    :type counts: list
    :param index: Index of the particle.
    :type index: int
    :returns: Pair ``(start, end)`` of positions in the flat branch.
    :rtype: tuple of int
    """
    offsets_method = getattr(event, "cumulative_offsets", None)
    if offsets_method is None:
        start = sum(counts[:index])
    else:
        start = offsets_method(counts_branch)[index]
    return start, start + counts[index]


def resolve_type_cast(text, attr):
    """Resolve the ``type:`` cast of an attribute to a callable.

    The cast is a Python expression evaluated in this module, exactly as
    :meth:`Particle._init_from_dict` does, so ``type: int`` keeps working.

    :param text: The expression held by the ``type`` field.
    :type text: str
    :param attr: The attribute name, for the error message.
    :type attr: str
    :returns: The object the expression evaluates to, usually a type.
    :rtype: object
    :raises ValueError: If the expression does not name a known object.
    """
    try:
        return eval(compile_expression(text))
    except NameError as e:
        raise ValueError(f"Invalid type '{text}' for attribute '{attr}'. Error: {e}")


def _literal_reader(value):
    """Reader for an attribute written as a literal in the config.

    Literals come from the config (``matched_tps: []``, ``showered: False``), so a
    container has to be copied for every particle: one list must not be shared by
    the whole collection.
    """

    if isinstance(value, (list, dict, set)):

        def read(ev, index, state, _value=value):
            return copy_literal(_value)

    else:

        def read(ev, index, state, _value=value):
            return _value

    return read


def _branch_reader(branch):
    """Reader for ``branch: name``: the column of the event, indexed by particle.

    A scalar branch is handed to every particle unchanged, as before.
    """

    def read(ev, index, state, _branch=branch):
        value = materialize(getattr(ev, _branch, None))
        if value is None:
            raise ValueError(f"Branch '{_branch}' not found in the event entry.")
        if isinstance(value, list):
            return value[index]
        return value

    return read


def _pair_reader(attr, flat_branch, counts_branch, resolution):
    """Reader for ``branch: [flat, counts]``: the slice of the flat column.

    ``resolution`` is the pair (target, identifier) when the pair is a cross
    reference. The reader then tags the attribute, so that Event resolves the raw
    identifiers into particles once every collection has been built.
    """

    def read(ev, index, state, _flat=flat_branch, _counts=counts_branch):
        flat_values = materialize(getattr(ev, _flat, None))
        counts = materialize(getattr(ev, _counts, None))
        if flat_values is None:
            raise ValueError(f"Branch '{_flat}' not found in the event entry.")
        if counts is None:
            raise ValueError(f"Companion branch '{_counts}' not found for '{_flat}'.")
        if index >= len(counts):
            raise IndexError(
                f"Particle index {index} out of bounds for '{_counts}' with length {len(counts)}"
            )
        if resolution is not None:
            state["_needs_resolution"][attr] = resolution
        start, end = flat_pair_bounds(ev, _counts, counts, index)
        return flat_values[start:end]

    return read


def _expression_reader(code):
    """Reader for ``expr: text``, evaluated with the values already set as locals.

    ``state`` is the dictionary of the particle being built, so an expression may
    use the attributes declared before it in the config (``detector_side`` from
    ``wh``), exactly as it does today.
    """

    def read(ev, index, state, _code=code):
        return eval(_code, {}, state)

    return read


def _cast_reader(reader, cast, attr, text):
    """Wrap a reader with the ``type:`` cast of the attribute."""

    def read(ev, index, state, _reader=reader, _cast=cast):
        value = _reader(ev, index, state)
        if not isinstance(value, list):
            try:
                return _cast(value)
            except TypeError as e:
                raise ValueError(
                    f"Cannot convert value '{value}' to type '{text}' for attribute '{attr}'. Error: {e}"
                )
        # A list is converted lazily, as Particle._init_from_dict does
        return map(_cast, value)

    return read


def attribute_reader(attr, spec):
    """Compile one attribute definition of the config into a reader.

    The reader is called as ``read(ev, index, state)`` and returns the value of the
    attribute for particle ``index`` of the event ``ev``. ``state`` is the dictionary of
    the particle being built, so an ``expr`` attribute sees the attributes declared
    before it, exactly as in ``Particle.__init__``. A literal definition returns a
    fresh copy for every particle.

    Event compiles one reader per attribute and per event, then passes them to
    ``Particle(readers=...)``, which calls each of them, instead of looking the
    definition up again for every particle.

    :param attr: The attribute name.
    :type attr: str
    :param spec: The attribute definition from the config: a dict with ``branch``,
        ``expr``, ``src`` and ``type``, or a literal.
    :type spec: dict or object
    :returns: The reader.
    :rtype: callable
    :raises ValueError: If the definition is not valid.
    """
    if not isinstance(spec, dict):
        return _literal_reader(spec)

    branch = spec.get("branch", None)
    expr = spec.get("expr", None)
    src = spec.get("src", None)
    cast = spec.get("type", None)

    provided = [value for value in (branch, expr, src) if value is not None]
    if len(provided) != 1:
        raise ValueError(
            f"'attributes' field must specify exactly one of 'branch', 'expr', or 'src' for attribute definition : '{attr}'."
        )

    if branch:
        if isinstance(branch, list):
            if len(branch) != 2:
                raise ValueError(
                    f"Branch list for attribute '{attr}' must contain exactly two entries: [flat, flat_counts]."
                )
            target = spec.get("target", None)
            resolution = None
            if target is not None:
                resolution = (target, spec.get("identifier", None))
            read = _pair_reader(attr, branch[0], branch[1], resolution)
        else:
            read = _branch_reader(branch)
    elif expr:
        try:
            read = _expression_reader(compile_expression(expr))
        except SyntaxError as e:
            raise ValueError(f"Invalid expression '{expr}' for attribute '{attr}'. Error: {e}")
    else:
        # A callable over the particle object: Particle.__init__ calls it, see Event
        raise ValueError(
            f"Attribute '{attr}' uses 'src', which is only supported by Particle.__init__."
        )

    if cast:
        read = _cast_reader(read, resolve_type_cast(cast, attr), attr, cast)
    return read


class Particle:
    """
    Base class to represent a particle.

    Attributes
    ----------
    index : int
        The index of the particle.
    name : str
        The name of the particle class. By default, it is the class name.
    """

    def __init__(self, index=None, ev=None, readers=None, **kwargs):
        """
        Initialize a Particle instance.

        :param index: The index of the particle.
        :type index: int
        :param ev: The TTree event entry containing event data.
        :param readers: Attribute readers already compiled for the event, from
            :func:`attribute_reader`. Event passes them to build a whole collection
            without interpreting the config again for every particle; when they are
            given, the definitions in ``kwargs`` are not used.
        :type readers: list
        :param kwargs: Additional attributes to set explicitly.
        """
        self.index = index
        self.name = self.__class__.__name__  # By default, it is the class name
        self._needs_resolution = {}  # Track attributes that need cross-reference resolution

        if readers is not None:
            self._init_from_readers(readers, ev)
            return

        for key, value in kwargs.items():
            if isinstance(value, dict):
                self._init_from_dict(key, value, event=ev)
            else:
                setattr(self, key, copy_literal(value))

    def _init_from_dict(self, attr, attr_info, event=None):
        """
        Initialize the particle attributes from a dictionary containing evaluable expressions, callable method, or branch names.

        :param attr: The attribute name to set.
        :param attr_info: A dictionary containing the information to set the attribute.
        """
        branch = attr_info.get("branch", None)
        expr = attr_info.get("expr", None)
        src = attr_info.get("src", None)
        kwargs = attr_info.get("kwargs", None)
        _type = attr_info.get("type", None)
        target = attr_info.get("target", None)  # Target collection for cross-reference resolution
        identifier = attr_info.get("identifier", None)  # identifier for cross-reference resolution

        # Ensure exactly one of 'branch', 'expr', or 'src' is provided
        provided = [x for x in [branch, expr, src] if x is not None]
        if len(provided) != 1:
            raise ValueError(
                f"'attributes' field must specify exactly one of 'branch', 'expr', or 'src' for attribute definition : '{attr}'."
            )

        is_two_branch_attr = False  # Flag to track if this is a two-branch attribute

        if branch:
            # If a branch is provided, set the attribute to the value from the event
            if event is None:
                raise ValueError(
                    f"Event must be provided to set attribute '{attr}' from branch '{branch}'."
                )

            if isinstance(
                branch, list
            ):  # Special handling for attributes defined by a pair of branches (flat + flat_counts)
                if len(branch) != 2:
                    raise ValueError(
                        f"Branch list for attribute '{attr}' must contain exactly two entries: [flat, flat_counts]."
                    )
                val_branch, counts_branch = branch
                flat_vals = materialize(getattr(event, val_branch, None))
                counts = materialize(getattr(event, counts_branch, None))
                if flat_vals is None:
                    raise ValueError(f"Branch '{val_branch}' not found in the event entry.")
                if counts is None:
                    raise ValueError(
                        f"Companion branch '{counts_branch}' not found for '{val_branch}'."
                    )

                if self.index >= len(counts):
                    raise IndexError(
                        f"Particle index {self.index} out of bounds for '{counts_branch}' "
                        f"with length {len(counts)}"
                    )
                start, end = flat_pair_bounds(event, counts_branch, counts, self.index)
                value = flat_vals[start:end]
                is_two_branch_attr = True  # Mark as two-branch attribute
            else:
                value = materialize(getattr(event, branch, None))
                if value is None:
                    raise ValueError(f"Branch '{branch}' not found in the event entry.")
                if isinstance(value, list):
                    # The event caches one materialized list per branch: index it
                    # instead of converting it again for every particle.
                    value = value[self.index]

        elif expr:
            try:
                value = eval(compile_expression(expr), {}, self.__dict__)
            except SyntaxError as e:
                raise ValueError(f"Invalid expression '{expr}' for attribute '{attr}'. Error: {e}")
        elif src:
            method = get_callable_from_src(src)
            if method is None:
                raise ImportError(f"Method '{src}' not found in module.")
            if kwargs:
                method = partial(method, **kwargs)
            value = method(self)

        if _type:
            try:
                _type = eval(compile_expression(_type))
                if not isinstance(value, list):
                    value = _type(value)
                else:
                    value = map(_type, value)
            except NameError as e:
                raise ValueError(f"Invalid type '{_type}' for attribute '{attr}'. Error: {e}")
            except TypeError as e:
                raise ValueError(
                    f"Cannot convert value '{value}' to type '{_type}' for attribute '{attr}'. Error: {e}"
                )

        # Tag for cross-reference resolution if this is a two-branch attribute with a target
        if is_two_branch_attr and target is not None:
            self._needs_resolution[attr] = (target, identifier)

        # Set the attribute to the value
        setattr(self, attr, value)

    def _init_from_readers(self, readers, ev):
        """Set the attributes from readers compiled for the event.

        The counterpart of :meth:`_init_from_dict` for the readers that
        :func:`attribute_reader` builds once per event. The instance dictionary
        is filled in config order, so an ``expr`` attribute sees the attributes
        declared before it, exactly as in the general path.

        :param readers: Pairs ``(attribute, read)`` in config order.
        :type readers: list
        :param ev: The event entry the attributes are read from.
        """
        state = self.__dict__
        index = self.index
        for attr, read in readers:
            state[attr] = read(ev, index, state)

    def __str__(self, indentLevel=0, include=None, exclude=None, **kwargs):
        """
        Generate a string representation of the Particle instance.

        :param indentLevel: The indentation level for the summary.
        :type indentLevel: int
        :param include: List of property names to include in the summary. If None, include all.
        :type include: list or None
        :param exclude: List of property names to exclude from the summary. If None, exclude none.
        :type exclude: list or None
        :param kwargs: Additional keyword arguments to pass to the ``color_msg`` function.
        :return: The particle summary.
        :rtype: str
        """
        summary = [
            color_msg(
                f"{self.name} {self.index} info -->",
                color=kwargs.pop("color", "yellow"),
                indentLevel=indentLevel,
                return_str=True,
                **kwargs,
            )
        ]

        properties = {
            key: value
            for key, value in self.__dict__.items()
            if not key in {"index", "name"}
            and not key.startswith("_")
            and (include is None or key in include)
            and (exclude is None or key not in exclude)
        }

        summary.append(
            color_msg(
                ", ".join([f"{key.capitalize()}: {value}" for key, value in properties.items()]),
                indentLevel=indentLevel + 1,
                return_str=True,
            )
        )
        return "\n".join(summary)

    def _comparable(self):
        """
        Attributes that take part in equality and hashing.

        Private attributes, the index and the name are excluded, and so is every
        container: a particle holds lists of other particles (``matched_tps``),
        which would recurse. Two particles compare equal when their scalar
        attributes are equal, and their containers do not make them differ.

        :return: The comparable attributes of the particle.
        :rtype: dict
        """
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith("_")
            and key not in {"index", "name"}
            and not isinstance(value, Iterable)
        }

    def __eq__(self, other):
        """
        Compare two Particle instances by their scalar attributes.

        Private attributes (such as the internal resolution bookkeeping) and the
        index and name are ignored, and so is every container attribute.

        :param other: The other Particle instance to compare.
        :type other: Particle
        :return: True if the instances are equal, False otherwise.
        :rtype: bool
        """
        if not isinstance(other, Particle):
            return NotImplemented

        return self._comparable() == other._comparable()

    def __hash__(self):
        """
        Generate a hash for the Particle instance from its scalar attributes.

        :return: The hash value of the Particle instance.
        :rtype: int
        """
        return hash(frozenset(self._comparable().items()))


if __name__ == "__main__":
    # case 1 : Directly set attributes
    particle = Particle(
        index=0, wh=-2, sc=1, st=1, detector_side={"expr": "'+z' if wh > 0 else '-z'"}
    )
    print(particle)
    # case 2 : Set attributes from a TTree event entry. Input file is a dt ntuple here
    import os

    # Example usage
    input_file = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../tests/ntuples/DTDPGNtuple_12_4_2_Phase2Concentrator_thr6_Simulation_99.root",
        )
    )
    from ROOT import TFile

    # first, create a TFile object to read the dt ntuple (this is not necessary by using NTuple Class)
    attributes = {
        "pt": {"branch": "gen_pt"},
        "eta": {"branch": "gen_eta"},
        "phi": {"branch": "gen_phi"},
        "charge": {"branch": "gen_charge"},
    }
    with TFile(input_file, "read") as ntuple:
        tree = ntuple["dtNtupleProducer/DTTREE;1"]
        for iev, ev in enumerate(tree):
            # in events of this ntuple, each event contains more o less 2 genmuons,
            # so, set index 0 or 1 allows to take properties of the first or second genmuon
            particle = Particle(index=0, ev=ev, name="GenMuon", **attributes)
            print(particle)
            break  # just to test the first event
