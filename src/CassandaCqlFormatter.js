

const { SqlFormatter, ObjectNameValidator, QueryEntity } = require('@themost/query');

const TRIM_QUALIFIED_NAME_REGEXP = /^(\w+)((\.(\w+))+)/;
const TRIM_SAME_ALIAS_REGEXP = /^(.*)\sAS\s(.*)$/;

const escapeSingleQuotes = /'/g;
const escapeDoubleQuotes = /([^\\])"/g;
const removeEscapedDoubleQuotes = /\\"/g;

const CqlDialectTypes = [
  ['Boolean', 'BOOLEAN'],
  ['Byte', 'BLOB(1)'],
  ['Number', 'FLOAT'],
  ['Number', 'FLOAT'],
  ['Counter', 'COUNTER'],
  ['Currency', 'DECIMAL(19,4)'],
  ['Decimal', 'DECIMAL(?,?)'],
  ['Date', 'DATE'],
  ['DateTime', 'TIMESTAMP'],
  ['Time', 'TIME'],
  ['Long', 'BIGINT'],
  ['Duration', 'DURATION'],
  ['Integer', 'INT'],
  ['Url', 'TEXT(?)'],
  ['Text', 'TEXT(?)'],
  ['Note', 'TEXT(?)'],
  ['Image', 'BLOB'],
  ['Binary', 'BLOB'],
  ['Guid', 'UUID'],
  ['Short', 'SMALLINT']
]


class CassandraCqlFormatter extends SqlFormatter {
  constructor() {
    super({
        nameFormat: '"$1"',
        forceAlias: false
    });
  }

  escapeName(name) {
    if (typeof name === 'object' && Object.prototype.hasOwnProperty.call(name, '$name')) {
        return this.escapeName(name.$name);
    }
    if (typeof name !== 'string') {
        throw new Error('Invalid name expression. Expected string.');
    }
    const matches = TRIM_QUALIFIED_NAME_REGEXP.exec(name);
    if (matches) {
        return this.escapeName(matches[matches.length - 1]);
    }
    return super.escapeName(name);
}

escapeEntity(name) {
    return super.escapeEntity(name);
}

  formatFieldEx(field, format) {
    if (Object.prototype.hasOwnProperty.call(field, '$name')) {
        const { $name: name } = field;
        const matches = TRIM_QUALIFIED_NAME_REGEXP.exec(name);
        if (matches) {
            return this.escapeName(matches[matches.length - 1]);
        }
    }
    const result = super.formatFieldEx(field, format);
    const matches = TRIM_SAME_ALIAS_REGEXP.exec(result);
    if (matches && matches[1] === matches[2]) {
        return matches[1];
    }
    return result;
}

  formatSelect(select) {
    // format $ref property which a reference to a table
    // because entity aliases are not supported by Cassandra and should be removed
    if (select.$ref) {
      // get first key
      const keys = Object.keys(select.$ref);
      // remove alias
      for(const key of keys) {
        const entity = select.$ref[key];
        if (entity && entity.name) {
          select.$ref = new QueryEntity(entity.name);
          break;
        }
      }
    }
    return super.formatSelect(select);
  }

  formatType(field) {
    const size = parseInt(field.size);
    const scale = parseInt(field.scale);
    const {type} = field;
    const cqlType = CqlDialectTypes.find((item) => item[0] === type);
    if (cqlType) {
        const sizeExpr = /\(\?\)/g;
        let resultType = cqlType[1];
        let matches = sizeExpr.exec(cqlType[1]);
        if (matches) {
            resultType = size > 0 ? cqlType[1].replace(sizeExpr, `(${size})`) : cqlType[1].replace(sizeExpr, '')
        }
        const sizeAndScaleExpr = /\(\?,\?\)/g;
        matches = sizeAndScaleExpr.exec(cqlType[1]);
        if (matches) {
            return size > 0 && scale >= 0 ? cqlType[1].replace(sizeAndScaleExpr, `(${size},${scale})`) : cqlType[1].replace(sizeAndScaleExpr, '');
        }
        if (field.many) {
          return `LIST<${resultType}>`;
        }
        return resultType;
    }
    throw new Error(`Type ${type} is not supported by Cassandra CQL Fromatter.`);
  }

  stringify(value, replacer, space) {
    const str = JSON.stringify(value, replacer, space);
    return str.replace(escapeSingleQuotes, '\\\'')
    .replace(escapeDoubleQuotes, '$1\'')
    .replace(removeEscapedDoubleQuotes, '"');
  }
  
}

export {
    CqlDialectTypes,
    CassandraCqlFormatter
}