# PyODBCache Examples

Many of the base topics are already covered in the [PyODB Examples](./PyODBExamples.md), therefore
only the topics specific to the cache are covered here:

## Creating a PyODBCache

To create a cache you simply need to import the `PyODBCache` class and create an instance. This
class' constructor takes nearly the same arguments as the normal `PyODB` class.
The only difference is that there is no `load_existing` argument.

```python
from pyodb import PyODBCache

cache = PyODBCache()
```

## Caching data

To cache data you need to define a new cache first. A cache is added via the `add_cache` method.
This method takes a key which is used to access the cache, a data function which is used to get and
refresh the cached data after it expires and the type that the data function returns.

Optionally you may also set the lifetime (expiry time) of the cache.

You may also specify `force=True` in which case the cache is overwritten if it already exists.
Otherwise when trying to re-define the cache it will throw an error.

```python
def generate_data() -> list[MyClass]:
    # generate and return a list of MyClass


cache.add_cache(
    cache_key = "test",
    data_func = generate_data,
    data_type = MyClass,
    lifetime = 30,
    force = False
)
```

## Getting cached data

> We will be using the cache from above here.

You may simply get data by either using the get_data function or by dict-like key access:

```python
data = cache.get_data("test")
data = cache["test"]
```

You may even specify a function that takes parameters as data function. In this case you lose the
ability to get data via key access because you need to pass the necessary arguments in the `get_data`
function.

Getting data from a paremterized function looks like this:

```python
def param_generate_data(amount: int) -> list[MyClass]:
    # Generates <amount> of MyClass instances and returns them as a list

# Add the new cache
cache.add_cache(
    cache_key = "param_test",
    data_func = param_generate_data,
    data_type = MyClass,
    lifetime = 30,
    force = False
)

# Get 5 MyClass instances
cache.get_data("param_test", amount=5)
```

## In-Memory Store

To optimize performance the PyODBCache uses an in-memory store for quick data access.
This in-memory store is used whenever data was already generated (or pulled from the database) and
the cache has not expired.

Once the cache expires it tries to pull fresh data from the database and load this into the
in-memory store. If this is not possible due to the data within the database being outdated, it
generates the data itself and saves it to the database.
The data is saved to the datbase to enable other cache instances to load it so they do not have to
re-generate the data themselves.

Depending on the amount of data cached this may cause a noticeable impact on the process' memory
consumption.
