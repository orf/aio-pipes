# Asyncio Pipes
![](https://travis-ci.org/orf/aio-pipes.svg?branch=master)

This module provides a set of pipe-like data structures for use with asyncio-based applications.

```python
def capitalize(name):
  return name.capitalize()
  
def say_hello(data):
  return "Hello {name}".format(name=data)

pipe = new Pipeline("My First Pipeline") | capitalize | say_hello
pipe < open("list_of_names.txt")
pipe > sys.stdout
asyncio.run_until_complete(pipe.start())
```

## Pipelines
Pipelines are composed of pipes and filters.
