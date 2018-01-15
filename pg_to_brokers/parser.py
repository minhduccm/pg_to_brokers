class Parser(object):

    def __init__(self):
        super(Parser, self).__init__()
        self.temp_str = 'abc123456789'

    def __parse_single_change(self, change_str, transaction_id):
        change_str = change_str.replace("''", self.temp_str)
        parts = change_str.split(' ', 3)
        change_type = parts[0]
        if change_type != 'table':
            return {'change_type': change_type}
        table_name = parts[1][:len(parts[1]) - 1]
        operation = parts[2][:len(parts[2]) - 1]
        record_change_str = parts[3]
        m_fields = []
        m_types = []
        m_values = []
        fields, types, values = self.__parse_change(
            record_change_str, m_fields, m_types, m_values
        )
        formatted_values = self.__format_by_types(types, values)
        res = {}
        res['transaction_id'] = transaction_id
        res['change_type'] = change_type
        res['table_name'] = table_name
        res['operation'] = operation
        res['fields'] = fields
        res['types'] = types
        res['values'] = formatted_values
        return res

    def __parse_field_and_type(self, field_and_type):
        parts = field_and_type.split('[')
        field = parts[0]
        type_of_field = parts[1][:len(parts[1]) - 1]
        return field, type_of_field

    def __parse_change(self, record_change_str, fields, types, values):
        temp_str = self.temp_str
        if record_change_str == '':
            return fields, types, values

        d1 = record_change_str.split(':', 1)
        field_and_type = d1[0]
        rest = d1[1]

        if rest[0] == "\'":  # value is string
            parts = rest.split("\'", 2)
            value = parts[1].replace(temp_str, "'")
            field, type_of_field = self.__parse_field_and_type(field_and_type)
            fields.append(field)
            types.append(type_of_field)
            values.append(value)
            if parts[2] == '':
                return self.__parse_change('', fields, types, values)
            # remove first char of part[2] because that's white space
            rest = parts[2][1:]
            return self.__parse_change(rest, fields, types, values)

        d2 = rest.split(' ', 1)
        value = d2[0]  # .replace(temp_str, "'")
        # in case of empty string value, we just have two single quotes
        if value == temp_str:
            value = ''
        if value == 'null':
            value = None
        field, type_of_field = self.__parse_field_and_type(field_and_type)
        fields.append(field)
        types.append(type_of_field)
        values.append(value)
        if len(d2) > 1:
            rest = d2[1]
            return self.__parse_change(rest, fields, types, values)
        return self.__parse_change('', fields, types, values)

    def __format_by_types(self, types, values):
        formatted_values = []
        for idx, current_type in enumerate(types):
            val = values[idx]
            if val is None:  # None type -> no need to cast type
                formatted_values.append(val)
            # integer, smallint, bigint, smallserial, serial, bigserial
            elif 'int' in current_type or 'serial' in current_type:
                formatted_values.append(int(val))
            elif 'bool' in current_type:
                formatted_val = True if val == 'true' else False
                formatted_values.append(formatted_val)
            elif 'decimal' in current_type \
                    or 'numeric' in current_type \
                    or 'double precision' in current_type \
                    or 'real' in current_type:
                formatted_values.append(float(val))
            else:
                formatted_values.append(val)
        return formatted_values

    def parse(self, changes, tables):
        formatted_changes = []
        for change in changes:
            transaction_id = change[1]
            change_str = change[2]
            change_json = self.__parse_single_change(
                change_str, transaction_id)
            if change_json['change_type'] != 'table':
                continue
            if len(tables) != 0 and change_json['table_name'] not in tables:
                continue
            formatted_changes.append(change_json)
        return formatted_changes
