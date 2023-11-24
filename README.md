[![PyPI version](https://badge.fury.io/py/ruson.svg)](https://badge.fury.io/py/ruson)

# Ruson-ODM

Ruson-ODM is an asynchronous object-document mapper for Python 3.11+. It is designed to be a lightweight and easy-to-use ODM for MongoDB.

## Installation

Ruson-ODM is available to be installed from PyPi with:

```bash
pip install ruson
``` 

## Usage

Using Ruson-ODM is easy and requires only two setup steps: setup and inheritance. After these two steps are complete, you can start using Ruson-ODM to query, insert, update and deleted documents from the database.

### Inheriting RusonDoc

`RusonDoc` is the interface that your classes will use to interact with the ODM. To use it, you must create a class that inherits from it.

```python
import asyncio
from ruson.core.config import Config
from ruson.core.instance import Ruson
from ruson.core.ruson_doc import RusonDoc


class User(RusonDoc):
    email: str
```

Note that subclasses can override super class attributes like the "id" field.

### Setup

To use the ruson ODM, you need to first create a `Config` instance with the necessary parameters for your database connection (`connection_name` is optional). With the config object created, you can then setup a `Ruson` connection using the singleton `Ruson`.

```python
async def setup_ruson():
    config = Config(database_uri="mongodb://localhost:27017", database_name="test", connection_name="default")
    await Ruson.create_connection(config)
```

### Querying the database

Once the `Ruson` connection is setup, you can start querying the database. Your classes that inherited from RusonDoc can now use the `find`, `find_one` and `find_many` methods to query the database.

By default, `Ruson` will not format your data. You can use the `formatter` parameter to pass a callable to format the response from the database.

```python
async def formatter(value: dict):
    # This function does not need to be async, it is just an
    # example to show that async functions are also supported
    return value["email"]


async def find():
    email = "test@example.com"

    # Return iterator over matched users
    users = await User(email=email).find(many=True)

    # Consumes iterator and transform into a list
    users_list = await users.tolist()

    print("Find method")
    print(f"Users found with email: '{email}'", len(users_list))
    print("Users", list(users_list[0].items()))
    print()


async def find_one():
    user_email = await User.find_one({"email": "test@example.com"}, formatter=formatter)
    print("Find one method")
    print("User email", user_email)
    print()


async def find_many():
    users_emails = await User.find_many(formatter=formatter)

    print("Find many method")
    # Iteration asynchronously through the results
    async for email in users_emails:
        print(email)

    print()
```

### Inserting into the database

To insert a document into the database, you can use the `insert_one` method either with an instance or a class.

```python
async def insert_one():
    print("Insert one method")

    # Insert using class method
    user = User(email="test@example.com")
    result = await User.insert_one(user)
    print("Inserted id", result.inserted_id)

    # Insert using instance method
    another = User(email="another@example.com")
    result = await another.insert()
    print("Inserted id", result.inserted_id)

    print()
```

To insert multiple documents into the database, you can use the `insert_many` method either with a class.

```python
async def insert_many():
    users = [
        User(email="test1@example.com"),
        User(email="test2@example.com"),
        User(email="test3@example.com"),
        User(email="test4@example.com"),
    ]
    result = await User.insert_many(users)

    print("Insert many method")
    print("Inserted ids", list(map(str, result.inserted_ids)))
    print()
```

### Updating records in the database

To update a record in the database, you can use the `update_one` method. This method takes two parameters, the filter and the update. The filter is used to find the document to update and the update is the data to update the document with. It can be used either with the class method or the instance method.

```python
async def update_one():
    result = await User.update_one(
        {"$set": {"email": "updated@example.com"}}, {"email": "test1@example.com"}
    )

    print("Update one method")
    print("Matched count", result.matched_count)
    print("Modified count", result.modified_count)
    print()
```

### Deleting records from the database

To delete a record from the database, you can use the `delete_one` method. This method takes a filter as a parameter and can be used either with the class method or the instance method.

```python
async def delete():
    # Delete only the first match using "many=False"
    result = await User(email="test4@example.com").delete(many=False)

    print("Delete method")
    print("Deleted count", result.deleted_count)
    print()


async def delete_one():
    # Delete class method
    result = await User.delete_one({"email": "test3@example.com"})

    print("Delete one method")
    print("Deleted count", result.deleted_count)
    print()
```

It is also possible to delete multiple records from the database using the `delete_many` method. This method takes a filter as a parameter and can be used either with the class method or the instance method `delete` combined with the flag `many=True`.

```python
async def delete_many():
    # Delete many using class method
    result = await User.delete_many({"email": {"$regex": "^test"}})

    print("Delete many method")
    print("Deleted count", result.deleted_count)
    print()
```

### To run the examples

```python
async def main():
    await setup_ruson()
    await insert_one()
    await insert_many()
    await find()
    await find_one()
    await find_many()
    await update_one()
    await delete()
    await delete_one()
    await delete_many()


if __name__ == "__main__":
    asyncio.run(main())
```

### Other supported methods

#### Ruson

-   `get_client`
-   `get_config`
-   `create_session`

#### Client

-   `database`
-   `default_database`
-   `list_databases`
-   `create_session`
-   `shutdown`

#### Database

-   `collection`
-   `list_collections`
-   `drop`

#### Collection

-   `upsert`
-   `count_documents`
-   `distinct`
-   `create_index`
-   `list_indexes`
-   `drop_indexes`
-   `drop`
