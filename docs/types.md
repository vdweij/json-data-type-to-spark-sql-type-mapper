# Supported Types

## String
A JSON string is converted by the mapper to a Spark **StringType**. 

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

## Object

# In progress

## DateTime
JSON does not include a built-in data type for representing date and time values. It however supports a `format` for a string type value. The following could be used that are all ISO 8601 format based:

- date-time "yyyy-MM-ddThh:mm:ssZ" could be parsed to **TimestampType**
- date "yyyy-MM-dd" could be parsed to **DateType**
- time "hh:mm:ssZ" would only fit a **TimestampType** in case a complete timestamp is constructed. This cannot be guessed upfront and as such it will be converted to a **StringType** instead.

# Unsupported (yet)

- Const
- Not
- anyOf
- allOf
- oneOf
- any
- null