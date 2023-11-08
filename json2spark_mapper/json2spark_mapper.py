import json
from pyspark.sql.types import StructType, StructField, ArrayType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, BooleanType, TimestampType, NullType

def map_json_schema_to_spark_schema(schema) -> StructType:
    properties = schema['properties']
    fields = []
    for key, value in properties.items():
        nullable = True
        field_type = None 
        if value['type'] == 'string':
            field_type = StringType()
        elif value['type'] == 'boolean':
            field_type = BooleanType() 
        elif value['type'] == 'integer':
            # This is tricky as there are many Spark types that can be mapped to an int
            field_type = convert_json_int(value)
        elif value['type'] == 'number':
            # This is also tricky as there are many Spark types that can be mapped to a number
            field_type = convert_json_number(value)
        elif value['type'] == 'array':
            if 'items' in value:
                items_schemas = value['items']
                
                # An array can have a single type or an array of types
                if isinstance(items_schemas, dict):
                    # put it into list
                    items_schemas = [items_schemas]
                    print("type items_schemas: ", type(items_schemas))
                
                # Check for size
                if len(items_schemas) < 1:
                    raise Exception("Expected a least one type definition in an array")
                
                # Loop over item schemas
                for item_schema in items_schemas:
                    if item_schema['type'] == 'object':
                        field_type = ArrayType(StructType(map_json_schema_to_spark_schema(item_schema).fields))
                    else:
                        #field_type = ArrayType(map_json_type_to_spark_type(item_schema['type']))
                        field_type = ArrayType(map_json_type_to_spark_type(item_schema))
        elif value['type'] == 'object':
            field_type = StructType(map_json_schema_to_spark_schema(value).fields)
        elif value['type'] == 'datetime':
            field_type = TimestampType()
        
        # check whether field is required
        if key in schema.get('required', []):
            nullable = False
        #
        # Setting nullable has no effect on the created DataFrame. This would be needed to be done afterwards.
        #
        # By default, when Spark reads a JSON file and infers the schema, it assumes that all fields are nullable.
        # If the actual data in the file contains null values for a field that was inferred as non-nullable,
        # Spark will coerce that field to be nullable, since it cannot guarantee that the field will always be non-null.
        
        fields.append(StructField(key, field_type, nullable))
    return StructType(fields)

def map_json_type_to_spark_type(value):
    json_type = value['type']
    field_type = None
    if json_type == 'string':
        field_type = StringType()
    elif json_type == 'integer':
        # This is tricky as there are many Spark types that can be mapped to an int
        # field_type = IntegerType()
        field_type = convert_json_int(value)
    elif json_type == 'number':
        # This is also tricky as there are many Spark types that can be mapped to a number
        # field_type = DoubleType()
        field_type = convert_json_number(value)
    elif json_type == 'datetime':
        field_type = TimestampType()
    else:
        raise ValueError(f"Invalid JSON type: {json_type}")
        
    return field_type

def convert_json_int(value):
    # This is tricky as there are many Spark types that can be mapped to an int
    # 
    # ByteType: Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127.
    # ShortType: Represents 2-byte signed integer numbers. The range of numbers is from -32768 to 32767.
    # IntegerType: Represents 4-byte signed integer numbers. The range of numbers is from -2147483648 to 2147483647.
    # LongType: Represents 8-byte signed integer numbers. The range of numbers is from -9223372036854775808 to 9223372036854775807.
    # 
    # https://spark.apache.org/docs/latest/sql-ref-datatypes.html
    #
    # For instance 20230214110547 fits in a json int, but not in a Spark IntegerType
    #
    field_type = LongType()
    determined_range = determine_inclusive_range(value)
    if (determined_range["defined"]):
        # max value of range is exclusive              
        byte_type_range = range(-128, 127 + 1)
        short_type_range = range(-32768, 32767 + 1)
        int_type_range = range(-2147483648, 2147483647 + 1)

        if (determined_range["min"] in byte_type_range and determined_range["max"] in byte_type_range):
            field_type = ByteType()
        elif (determined_range["min"] in short_type_range and determined_range["max"] in short_type_range):
            field_type = ShortType()  
        elif (determined_range["min"] in int_type_range and determined_range["max"] in int_type_range):
            field_type = IntegerType()
            
    return field_type

def convert_json_number(value):
    # This is also tricky as there are many Spark types that can be mapped to a number
    #
    # - FloatType: Represents 4-byte single-precision floating point numbers.
    # - DoubleType: Represents 8-byte double-precision floating point numbers.
    #
    # And optionally
    # - DecimalType: Represents arbitrary-precision signed decimal numbers. Backed internally by java.math.BigDecimal. 
    #   A BigDecimal consists of an arbitrary precision integer unscaled value and a 32-bit integer scale.
    # 
    # https://spark.apache.org/docs/latest/sql-ref-datatypes.html
    #
    #
    field_type = DoubleType()
    # There is no way to know to purpose of the value. To be on the safe side use DoubleType
    return field_type
    
def determine_inclusive_range(value):
    range = {"min": None, "max": None, "defined": False}
    
    if "minimum" in value:
        range["min"] = int(value["minimum"])
    if "exclusiveMinimum" in value:
        range["min"] = int(value["exclusiveMinimum"]) - 1   
    if "maximum" in value:
        range["max"] = int(value["maximum"])
    if "exclusiveMaximum" in value:
        range["max"] = int(value["exclusiveMaximum"]) - 1

    if range["min"] != None and range["max"] != None:
        range["defined"] = True

    return range
