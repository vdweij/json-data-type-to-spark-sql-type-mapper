# Supported Types

## String
A JSON string is converted by the mapper to a Spark **StringType**.

### DateTime

JSON does not include a built-in data type for representing date and time values. It however supports a `format` for a string type value. The following could be used, which are all ISO 8601 format based:

- date-time "yyyy-MM-ddThh:mm:ssZ"
- date "yyyy-MM-dd"
- time "hh:mm:ssZ"

In case a `date-time` or `date` format is specified it will be converted in respectively a **TimestampType** and a **DateType**. The `time`` format is ignored as it could only fit a **TimestampType** when prepended with a date part. This cannot be guessed upfront and as such it will be converted to a **StringType** instead.

Below is an example of `date-time` format:
```json
{
  "type": "object",
  "properties": {
    "dateOfBirth": {
      "type": "string",
      "format": "date-time"
    }
  }
}
```

## Boolean
A JSON boolean is converted by the mapper to a Spark **BooleanType**. 

## Integer
A JSON integer is a data type used to represent whole numbers. JSON integers can theoretically represent any whole number within the limits of the data type used by the programming language or system that processes the JSON data. However, JSON itself does not set constraints on the range of integers it can represent.
Apache Spark on the other hand has the following type that can accommodate whole numbers:
- The **ByteType** (Byte) represents 8-bit signed integers, which have the smallest range of all Spark integer types. It can store whole numbers within the range of approximately -128 to +127.
- The **ShortType** (Short) represents 16-bit signed integers, which have a more limited range compared to IntegerType and LongType. It can store whole numbers within the range of approximately -32,768 to +32,767.
- The **IntegerType** (Int) represents 32-bit signed integers, which can store whole numbers within the range of approximately -2 billion to +2 billion. This is one of the most commonly used data types for whole numbers in Spark.
- The **LongType** (Long) represents 64-bit signed integers, allowing for a much larger range. It can store whole numbers within the range of approximately -9 quintillion to +9 quintillion. This data type is suitable for very large whole numbers.

By either specifying a 'range' in the JSON schema the mapper can select the best matching Spark Type.

```json
{
  "type": "object",
  "properties": {
    "age": {
      "type": "integer",
      "minimum": 18,
      "maximum": 120
    }
  }
}
```
Also the `exclusiveMinimum` and `exclusiveMaximum` keywords are supported that can also be used to specify the range.


## Number
A JSON number could be converted both in a **FloatType** and a **DoubleType** or even into a **DecimalType**. A JSON schema however lacks the possibility to define the purpose of the number in a standard manner, hence for safety it is converted into a **DoubleType**.

## Array
A JSON array can hold a single type or a set of types. It is possible to nest arrays in arrays.

- It is possible to define a range with minItems and maxItems
- It is possible to define anyOf idicating which types are allowed
- It is possible to define a structure with a specific order, size and types

JSON arrays can be represented using the **ArrayType** in Spark SQL. It is used to represent arrays or lists of elements. The elements within the array can have various data types, and Spark provides flexibility in handling arrays of different types.

## Object
In JSON an `object` has fields with a name and a corresponding data type. It is possible to nest objects in objects to create complex representations. The type can be mapped to a **StructType** in Spark SQL that is also capable of containing fields of various types.

## Null
In JSON, the null value represents the absence of a value. It is used to indicate that a JSON property or element does not have a value or is undefined. In case present the mapper converts it into a **NullType**.

## Any
In JSON, the any type indicates an absence of constraints on the data type. The closest equivalent in Spark to an "any" type would be to use a more permissive data type like **StringType**. 

## AnyOf
In JSON, the `anyOf` keyword indicates that the value must match any of the given types. The only safe type to convert it to would be **StringType**.


## Const
Although not a JSON type, the keyword `const` specifies a constant property's value that the corresponing JSON must exactly match. It can contain all sorts of value types, even complex data type, hence converting it to **StringType** is the safest option.

# Unsupported (yet)

- not (used for values)
- allOf (used for a set of required properties)
- oneOf (used for matching values)