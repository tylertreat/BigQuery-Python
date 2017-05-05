from logging import getLogger, NullHandler

logger = getLogger(__name__)
logger.addHandler(NullHandler())


def render_query(dataset, tables, select=None, conditions=None,
                 groupings=None, having=None, order_by=None, limit=None):
    """Render a query that will run over the given tables using the specified
    parameters.

    Parameters
    ----------
    dataset : str
        The BigQuery dataset to query data from
    tables : Union[dict, list]
        The table in `dataset` to query.
    select : dict, optional
        The keys function as column names and the values function as options to
        apply to the select field such as alias and format.  For example,
        select['start_time'] might have the form
        {'alias': 'StartTime', 'format': 'INTEGER-FORMAT_UTC_USEC'}, which
        would be represented as 'SEC_TO_TIMESTAMP(INTEGER(start_time)) as
        StartTime' in a query. Pass `None` to select all.
    conditions : list, optional
        a ``list`` of ``dict`` objects to filter results by.  Each dict should
        have the keys 'field', 'type', and 'comparators'. The first two map to
        strings representing the field (e.g. 'foo') and type (e.g. 'FLOAT').
        'comparators' maps to another ``dict`` containing the keys 'condition',
        'negate', and 'value'.
        If 'comparators' = {'condition': '>=', 'negate': False, 'value': 1},
        this example will be rdnered as 'foo >= FLOAT('1')' in the query.
        ``list`` of field names to group by
    order_by : dict, optional
        Keys = {'field', 'direction'}. `dict` should be formatted as
        {'field':'TimeStamp, 'direction':'desc'} or similar
    limit : int, optional
        Limit the amount of data needed to be returned.

    Returns
    -------
    str
        A rendered query
    """

    if None in (dataset, tables):
        return None

    query = "%s %s %s %s %s %s %s" % (
        _render_select(select),
        _render_sources(dataset, tables),
        _render_conditions(conditions),
        _render_groupings(groupings),
        _render_having(having),
        _render_order(order_by),
        _render_limit(limit)
    )

    return query


def _render_select(selections):
    """Render the selection part of a query.

    Parameters
    ----------
    selections : dict
        Selections for a table

    Returns
    -------
    str
        A string for the "select" part of a query

    See Also
    --------
    render_query : Further clarification of `selections` dict formatting
    """

    if not selections:
        return 'SELECT *'

    rendered_selections = []
    for name, options in selections.items():
        if not isinstance(options, list):
            options = [options]

        original_name = name
        for options_dict in options:
            name = original_name
            alias = options_dict.get('alias')
            alias = "as %s" % alias if alias else ""

            formatter = options_dict.get('format')
            if formatter:
                name = _format_select(formatter, name)

            rendered_selections.append("%s %s" % (name, alias))

    return "SELECT " + ", ".join(rendered_selections)


def _format_select(formatter, name):
    """Modify the query selector by applying any formatters to it.

    Parameters
    ----------
    formatter : str
       Hyphen-delimited formatter string where formatters are
       applied inside-out, e.g. the formatter string
       SEC_TO_MICRO-INTEGER-FORMAT_UTC_USEC applied to the selector
       foo would result in FORMAT_UTC_USEC(INTEGER(foo*1000000)).
    name: str
        The name of the selector to apply formatters to.

    Returns
    -------
    str
        The formatted selector
    """

    for caster in formatter.split('-'):
        if caster == 'SEC_TO_MICRO':
            name = "%s*1000000" % name
        elif ':' in caster:
            caster, args = caster.split(':')
            name = "%s(%s,%s)" % (caster, name, args)
        else:
            name = "%s(%s)" % (caster, name)

    return name


def _render_sources(dataset, tables):
    """Render the source part of a query.

    Parameters
    ----------
    dataset : str
        The data set to fetch log data from.
    tables : Union[dict, list]
        The tables to fetch log data from

    Returns
    -------
    str
        A string that represents the "from" part of a query.
    """

    if isinstance(tables, dict):
        if tables.get('date_range', False):
            try:
                dataset_table = '.'.join([dataset, tables['table']])
                return "FROM (TABLE_DATE_RANGE([{}], TIMESTAMP('{}'),"\
                    " TIMESTAMP('{}'))) ".format(dataset_table,
                                                 tables['from_date'],
                                                 tables['to_date'])
            except KeyError as exp:
                logger.warn(
                    'Missing parameter %s in selecting sources' % (exp))

    else:
        return "FROM " + ", ".join(
            ["[%s.%s]" % (dataset, table) for table in tables])


def _render_conditions(conditions):
    """Render the conditions part of a query.

    Parameters
    ----------
    conditions : list
        A list of dictionay items to filter a table.

    Returns
    -------
    str
        A string that represents the "where" part of a query

    See Also
    --------
    render_query : Further clarification of `conditions` formatting.
    """

    if not conditions:
        return ""

    rendered_conditions = []

    for condition in conditions:
        field = condition.get('field')
        field_type = condition.get('type')
        comparators = condition.get('comparators')

        if None in (field, field_type, comparators) or not comparators:
            logger.warn('Invalid condition passed in: %s' % condition)
            continue

        rendered_conditions.append(
            _render_condition(field, field_type, comparators))

    if not rendered_conditions:
        return ""

    return "WHERE %s" % (" AND ".join(rendered_conditions))


def _render_condition(field, field_type, comparators):
    """Render a single query condition.

    Parameters
    ----------
    field : str
        The field the condition applies to
    field_type : str
        The data type of the field.
    comparators : array_like
        An iterable of logic operators to use.

    Returns
    -------
    str
        a condition string.
    """

    field_type = field_type.upper()

    negated_conditions, normal_conditions = [], []

    for comparator in comparators:
        condition = comparator.get("condition").upper()
        negated = "NOT " if comparator.get("negate") else ""
        value = comparator.get("value")

        if condition == "IN":
            if isinstance(value, (list, tuple, set)):
                value = ', '.join(
                    sorted([_render_condition_value(v, field_type)
                            for v in value])
                )
            else:
                value = _render_condition_value(value, field_type)
            value = "(" + value + ")"
        elif condition == "BETWEEN":
            if isinstance(value, (tuple, list, set)) and len(value) == 2:
                value = ' AND '.join(
                    sorted([_render_condition_value(v, field_type)
                            for v in value])
                )
            elif isinstance(value, (tuple, list, set)) and len(value) != 2:
                logger.warn('Invalid condition passed in: %s' % condition)

        else:
            value = _render_condition_value(value, field_type)

        rendered_sub_condition = "%s%s %s %s" % (
            negated, field, condition, value)

        if comparator.get("negate"):
            negated_conditions.append(rendered_sub_condition)
        else:
            normal_conditions.append(rendered_sub_condition)

    rendered_normal = " AND ".join(normal_conditions)
    rendered_negated = " AND ".join(negated_conditions)

    if rendered_normal and rendered_negated:
        return "((%s) AND (%s))" % (rendered_normal, rendered_negated)

    return "(%s)" % (rendered_normal or rendered_negated)


def _render_condition_value(value, field_type):
    """Render a query condition value.

    Parameters
    ----------
    value : Union[bool, int, float, str, datetime]
        The value of the condition
    field_type : str
        The data type of the field

    Returns
    -------
    str
        A value string.
    """

    # BigQuery cannot cast strings to booleans, convert to ints
    if field_type == "BOOLEAN":
        value = 1 if value else 0
    elif field_type in ("STRING", "INTEGER", "FLOAT"):
        value = "'%s'" % (value)
    elif field_type in ("TIMESTAMP"):
        value = "'%s'" % (str(value))
    return "%s(%s)" % (field_type, value)


def _render_groupings(fields):
    """Render the group by part of a query.

    Parameters
    ----------
    fields : list
        A list of fields to group by.

    Returns
    -------
    str
        A string that represents the "group by" part of a query.
    """

    if not fields:
        return ""

    return "GROUP BY " + ", ".join(fields)


def _render_having(having_conditions):
    """Render the having part of a query.

    Parameters
    ----------
    having_conditions : list
        A ``list`` of ``dict``s to filter the rows

    Returns
    -------
    str
        A string that represents the "having" part of a query.

    See Also
    --------
    render_query : Further clarification of `conditions` formatting.
    """
    if not having_conditions:
        return ""

    rendered_conditions = []

    for condition in having_conditions:
        field = condition.get('field')
        field_type = condition.get('type')
        comparators = condition.get('comparators')

        if None in (field, field_type, comparators) or not comparators:
            logger.warn('Invalid condition passed in: %s' % condition)
            continue

        rendered_conditions.append(
            _render_condition(field, field_type, comparators))

    if not rendered_conditions:
        return ""

    return "HAVING %s" % (" AND ".join(rendered_conditions))


def _render_order(order):
    """Render the order by part of a query.

    Parameters
    ----------
    order : dict
        A dictionary with two keys, fields and direction.
        Such that the dictionary should be formatted as
        {'fields': ['TimeStamp'], 'direction':'desc'}.

    Returns
    -------
    str
        A string that represents the "order by" part of a query.
    """

    if not order or 'fields' not in order or 'direction' not in order:
        return ''

    return "ORDER BY %s %s" % (", ".join(order['fields']), order['direction'])


def _render_limit(limit):
    """Render the limit part of a query.

    Parameters
    ----------
    limit : int, optional
        Limit the amount of data needed to be returned.

    Returns
    -------
    str
        A string that represents the "limit" part of a query.
    """
    if not limit:
        return ''

    return "LIMIT %s" % limit
