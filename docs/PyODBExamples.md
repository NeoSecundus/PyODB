# PyODB Examples

1. [Choosing what to save](#choosing-what-to-save)
2. [Post-Processing of loaded instances](#post-processing-of-loaded-instances)
3. [Sharding](#sharding)
4. [The max depth](#the-max-depth)
5. [Logging](#logging)
6. [Persistency](#persistency)
7. [Performance Considerations](#performance-considerations)
8. [Restrictions](#restrictions)
9. [Other](#other)

## Choosing what to save

Custom types can have a lot of member variables and attributes. Some we want to save, some not.
To choose which members you want to save there are two possible ways.

The first way is to annotate the members you want to save as class variables. Members that are
solely defined in the `__int__` method and do not have a definition in the class scope are ignored.

```python
class MyClass:
    save_me: str
    _me_too: int

    def __init__(self):
        self.save_me = "Hello"
        self._me_too = 123
        self.ignore_me = "I will be ignored"
```

In case you have class members that you do not want to save or members that are declared in
`__init__` that you do not want to be visible in the class scope you may define the `_odb_members_`
dict.

`__odb_members__` always takes precedence over normal type annotations. The dict itself contains the
names of member variables as keys and the type(s) as values:

```python
class MyClass:
    ignore_me: str
    __odb_members__: dict[str, type] = {
        "save_me": str,
        "_me_too": str | None
    }

    def __init__(self):
        self.save_me = "I'm part of the odb members"
        self._me_too = "I will be saved too"
        self.ignore_me = "Ignored even though I'm declared in the class scope!"
```

## Post-Processing of loaded instances

In case there are members that cannot be saved, database connection instances for example, you may
define a method called `__odb_reassemble__` which is executed right after the base instance has been
reconstructed. `__odb_reassemble__` takes no arguments besides `self`.

```python
class NeedReassembly:
    name: str
    was_reassembled: bool

    def __init__(self):
        self.connection = DatabaseConnection()
        self.name = "Test reassemble"
        self.was_reassembled = False


    def __odb_reassemble__(self):
        self.connection = DatabaseConnection()
        self.was_reassembled = True
```

## Sharding

Sharding is a method of splitting the database into multiple files. One per type. This splitting
allows PyODB to make multiple write operations concurrently on different types. Otherwise write
operations always block the main database and other processes have to wait for it to complete.

This is mostly unnecessary for programs which only run on one thread. But for threaded or
multiprocessing programs this can lead to a dramatic performance improvement.

To activate sharding you simply have to set `sharding` to `True` when creating the PyODB instance.

```python
pyodb = PyODB(sharding=True)
```

## The max depth

The maximum recursion depth can be set at the start and in the middle of execution. In essence
it determines when PyODB starts pickling instances and saving them as BLOBs instead of creating
a child instance.

In the case that the max depth is changed while the program is already running all subsequent
inserts will use the new depth. Old data (inserted with the old max depth) can still be loaded
as before.

The max depth may be set when creating an instance:

```python
pyodb = PyODB(max_depth=3)
```

## Logging

PyODB does provide a logging module which can be activated when creating the instance:

```python
pyodb = PyODB(write_log=True)
```

In case you want to see the log messages on the console as well you can do:

```python
pyodb = PyODB(write_log=True, log_to_console=True)
```

## Persistency

By default the database is deleted when the process exits. In cases where the data needs to be
persistet you can set the `persistent` parameter.

```python
pyodb = PyODB(persistent=True)
```

The whole schema will be reloaded in case a new pyodb instance is created, so you can use the data
without having to add the types again. If you do not want this you may suppress loading by using the
`load_existing` parameter.

```python
pyodb = PyODB(load_existing=False)
```

> Notice: The schema will be re-loaded when an old schema exists even if `persistent` is set to False

## Performance Considerations

**Recursion depth:**
The writing and loading of data gets slower and slower the deeper the recursion is. It is advised to
set the max recursion depth as low as possible to minimize performance loss.

**Reconstruct instead of Save:**
It is also advised to drop any type members which could be easily reconstructed. You do not
necessarily need to save the result of a calculation if you can simply re-calculate it after the
instance was created.

## Restrictions

**Schema loading:**
When a type is changed the old type cannot be loaded from the database. It is necessary to delete
the schema definition every time types are changed. You may alternatively remove the reloaded types
manually and re-define them.

**Type Removal:**
Types cannot be removed from the schema when a parent type depends on it.

## Other

**Insert vs. Insert Many:**
Insert also adds the type to the schema in case it is not yet known.
Insert many does not do this - additionally the list passed into insert_many may only contain
members of the same type or type-union.

**Select/Delete restrictions:**
Select and delete filters cannot be used to filter by values contained by the sub-type.

Let's say there is a class High and a class Low:

```python
class Low:
    low_var: str

class Top:
    top_var: str
    low_instance: Low
```

You cannot select the Top class and use the filter on the `low_instance` member. So this cannot be
done:

```python
pyodb = PyODB()
select = pyodb.select()
select.eq(low_var="some_val") # <- NOT POSSIBLE
select.all()
```
