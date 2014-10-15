import logging


def render_query(dataset, tables, select=None, conditions=None,
                 groupings=None, order_by=None):
    """Render a query that will run over the given tables using the specified
    parameters.

    Args:
        dataset: the BigQuery data set to query data from.
        tables: the tables in dataset to query.
        select: a dictionary of selections for a table. The keys function as
                column names and the values function as options to apply to
                the select field such as alias and format.  For example,
                    {
                        'start_time': {
                            'alias': 'StartTime',
                            'format': 'INTEGER-FORMAT_UTC_USEC'
                        }
                    }
                is represented as 'SEC_TO_TIMESTAMP(INTEGER(start_time)) as
                StartTime' in a query. Pass None to select all.
        conditions: a list of dicts to filter results by.
                    Each dict should be formatted as the following:
                        {
                            'field': 'foo',
                            'type': 'FLOAT',
                            'comparators': [
                                {
                                    'condition': '>=',
                                    'negate': False,
                                    'value': '1'
                                }
                            ]
                        }
                    which is rendered as 'foo >= FLOAT('1')' in the query.
        groupings: a list of field names to group by.
        order_by: a dict with two keys, field and direction.
            Such that the dictionary should be formatted as
            {'field':'TimeStamp, 'direction':'desc'}.

    Returns:
        a query string.
    """

    if None in (dataset, tables):
        return None

    query = "%s %s %s %s %s" % (
        _render_select(select),
        _render_sources(dataset, tables),
        _render_conditions(conditions),
        _render_groupings(groupings),
        _render_order(order_by),
    )

    return query


def _render_select(selections):
    """Render the selection part of a query.

    Args:
        selections: a dictionary of selections for a table. The
            keys function as column names and the values function as
            options to apply to the select field such as alias and format.
            For example {'start_time': {'alias': 'StartTime', 'format':
            'INTEGER-FORMAT_UTC_USEC'}} is represented as
            'SEC_TO_TIMESTAMP(INTEGER(start_time))' in a query. Pass None to
            select all.

    Returns:
        a string that represents the select part of a query.
    """

    if not selections:
        return 'SELECT *'

    rendered_selections = []
    for name, options in selections.iteritems():
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

    Args:
        formatter: hyphen-delimited formatter string where formatters are
                   applied inside-out, e.g. the formatter string
                   SEC_TO_MICRO-INTEGER-FORMAT_UTC_USEC applied to the selector
                   foo would result in FORMAT_UTC_USEC(INTEGER(foo*1000000)).
        name: the name of the selector to apply formatters to.

    Returns:
        formatted selector.
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

    Args:
        dataset: the data set to fetch log data from.
        tables: the tables to fetch log data from.

    Returns:
        a string that represents the from part of a query.
    """

    return "FROM " + ", ".join(
        ["[%s.%s]" % (dataset, table) for table in tables])


def _render_conditions(conditions):
    """Render the conditions part of a query.

    Args:
        conditions: a list of dictionary items to filter a table.
            Each dict should be formatted as {'field': 'start_time',
            'value': {'value': 1, 'negate': False}, 'comparator': '>',
            'type': 'FLOAT'} which is represetned as
            'start_time > FLOAT('1')' in the query.

    Returns:
        a string that represents the where part of a query.
    """

    if not conditions:
        return ""

    rendered_conditions = []

    for condition in conditions:
        field = condition.get('field')
        field_type = condition.get('type')
        comparators = condition.get('comparators')

        if None in (field, field_type, comparators) or not comparators:
            logging.warn('Invalid condition passed in: %s' % condition)
            continue

        rendered_conditions.append(
            _render_condition(field, field_type, comparators))

    if not rendered_conditions:
        return ""

    return "WHERE %s" % (" AND ".join(rendered_conditions))


def _render_condition(field, field_type, comparators):
    """Render a single query condition.

    Args:
        field: the field the condition applies to.
        field_type: the data type of the field.
        comparator: the logic operator to use.
        value_dicts: a list of value dicts of the form
                     {'value': 'foo', 'negate': False}

    Returns:
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
                    [_render_condition_value(v, field_type) for v in value]
                )
            else:
                value = _render_condition_value(value, field_type)
            value = "(" + value + ")"
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

    Args:
        value: the value of the condition.
        field_type: the data type of the field.

    Returns:
        a value string.
    """

    # BigQuery cannot cast strings to booleans, convert to ints
    if field_type == "BOOLEAN":
        value = 1 if value else 0
    elif field_type in ("STRING", "INTEGER", "FLOAT"):
        value = "'%s'" % (value)
    return "%s(%s)" % (field_type, value)


def _render_order(order):
    """Render the order by part of a query.

    Args:
        order: a dictionary with two keys, field and direction.
            Such that the dictionary should be formatted as
            {'field':'TimeStamp, 'direction':'desc'}.

    Returns:
        a string that represents the order by part of a query.
    """

    if not order or 'field' not in order or 'direction' not in order:
        return ''

    return "ORDER BY %s %s" % (order['field'], order['direction'])


def _render_groupings(fields):
    """Render the group by part of a query.

    Args:
        fields: a list of fields to group by.

    Returns:
        a string that represents the group by part of a query.
    """

    if not fields:
        return ""

    return "GROUP BY " + ", ".join(fields)
