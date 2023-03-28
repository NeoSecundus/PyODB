# PyODB

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/NeoSecundus/PyODB/python-package.yml)

Python Object DataBase (PyODB) is a SQLite3 ORM library aiming to be as simple to use as possible.
This library is supposed to be used for testing, in small projects or for local inter-process data caching.

1. [Setup](#setup)
2. [Usage](#usage)
3. [Limitations](#limitations)
4. [How it works](#how-it-works)
   1. [Converting Primitive Types](#converting-primitive-types)
   2. [Converting Complex and Custom Types](#converting-complex-and-custom-types)
      1. [Simple custom type](#simple-custom-type)
      2. [Custom types contained in a list or set](#custom-types-contained-in-a-list-or-set)
      3. [Custom types contained in a dict](#custom-types-contained-in-a-dict)
   3. [What about dynamic types?](#what-about-dynamic-types)
   4. [What about fields containing functions?](#what-about-fields-containing-functions)

## Setup

<<< **WIP** >>>

## Usage

<<< **WIP** >>>

## Limitations

What this library can store:
- Primitive Datatypes (int, float, str, bool)
- Lists
- Dictionaries
- Custom Types

What this library cannot store:
- Fields containing functions (maybe in the future...)
- Fields that are dependent on external states or state changes

**Important Notice:**

Storing of dynamic type imported from external libraries should be avoided.
Depending of how deep an library type is nested one single datapoint could lead to the creation of
dozens of tables. One for every nested sub-type.

As example -> Storing a `PoolManager` from `urllib3` would result in these tables:
- PoolManager
- Url
- ProxyConfig
  - SSLContext
    - SSLObject
    - SSLSocket
      - SSLSession
      - ...
- ConnectionPool
  - LifoQueue
    - Queue
    - ...

This is still a very basic example. As a rule of thumb; The higher level the library the deeper the
objects are nested on average.

## How it works

When a python class is added to the PyODB schema the classes are converted to SQL Tables.
These tables are then inserted into the SQLite3 database on first use.

> IMPORTANT: If a class changes between executions the table is dropped and recreated. The table's
> data is lost.

The UML Diagram below shows what such a conversion looks like:

![Conversion Diagram](./docs/img/conversion_example.svg)

### Converting Primitive Types

Primitive python datatypes are simply converted to SQL datatypes.

| Python Datatype | SQL Datatype |
|-----------------|--------------|
| int | INTEGER |
| float | REAL |
| bool | NUMERIC |
| str | TEXT |
| datetime | NUMERIC |
| list[p] | TEXT (JSON) |
| dict[p, p] | TEXT (JSON) |

*p = Primitive Datatype (float, int, str, bool)

### Converting Complex and Custom Types

There is a certain amount of scenarios when converting complex and custom types. Below is a list of
all handled scenarios and the corresponding rules that deal with them. All of these scenarios are
also depicted in the example diagram above.

Custom and complex types are handled in the same way so only Custom Types are referenced from here
on out.

#### Simple custom type

When a simple custom type is contained by the origin class a table is created for the custom type as
well. The custom type's table then gets a `_parent_` column which references the origin class.

The origin class gets a `_children_` column which contains a JSON string containing this custom type
(or multiple) in the format: `{<Variable Name>: <Custom Type Classname>}`

#### Custom types contained in a list or set

This case is handled very similarly to the [Simple custom type](#simple-custom-type). The only
difference being the format of the entry within the `_children_` column. The format is changed to
contain a `l` before the Custom Type's Class-Name to indicate that this is supposed to be a list.
This is called a containment specifier:
`{<Variable Name>: l<Custom Type Classname>}`

Sets are the same but with a `s` instead of a `l`:
`{<Variable Name>: s<Custom Type Classname>}`

#### Custom types contained in a dict

Once again this case is handled similarly to the [Custom types contained in a list or set](#custom-types-contained-in-a-list-or-set).
There are two notable differences though:

- The containment specifier is changed to a `d`
  `{<Variable Name>: d<Custom Type Classname>}`
- The custom type receives a `_key_` column which contains the name of it's key or a dict containing
  the definition of the custom class that references it in the same format as the [Simple custom type](#simple-custom-type).

### What about dynamic types?

Dynamic type definitions can be saved by PyODB.
Only primitive datatypes must be statically defined.

The only exception is None. Data may always be defined with None as an option.

**This is allowed:**
```python
class Person:
    name: str
    dob: int
    info: str | None
    card: CreditCard | Bankcard | None
```

**This is not:**
```python
class Square:
    length: int | float
```

**Important Notice:**
A table will be created for every possible Datatype!

### What about fields containing functions?

<<< **WIP** >>>
