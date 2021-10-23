from redis import Redis

from flypper.entities.flag import UnversionedFlagData
from flypper_redis.storage.redis import RedisStorage

redis = Redis(host="localhost", port=6379, db=0)

def test_empty_storage():
    storage = empty_storage()
    assert storage.list() == []

def test_upsert():
    storage = empty_storage()
    storage.upsert(flag_data("a"))
    flags = storage.list()
    assert len(flags) == 1
    assert flags[0].name == "a"

def test_upsert_with_increasing_versions():
    storage = empty_storage()
    storage.upsert(flag_data("a"))
    storage.upsert(flag_data("b"))
    flags = storage.list()
    assert flags[0].version == 1
    assert flags[0].name == "a"
    assert flags[1].version == 2
    assert flags[1].name == "b"

def test_list_with_version():
    storage = empty_storage()
    storage.upsert(flag_data("a"))
    assert len(storage.list(version__gt=0)) == 1
    assert len(storage.list(version__gt=1)) == 0

def test_delete():
    storage = empty_storage()
    storage.upsert(flag_data("a"))
    storage.delete(flag_name="a")
    assert len(storage.list()) == 0


def empty_storage():
    # Cleaning up redis
    for key in redis.scan_iter(match="flypper-test:*"):
        redis.delete(key)

    return RedisStorage(redis=redis, prefix="flypper-test")

def flag_data(name: str) -> UnversionedFlagData:
    return {
        "name": name,
        "enabled": True,
        "enabled_for_actors": {
            "actor_key": "user_id",
            "actor_ids": ["8", "42", "200000"],
        },
        "enabled_for_percentage_of_actors": None,
        "deleted": False,
    }
