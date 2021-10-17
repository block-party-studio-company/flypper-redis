from redis import Redis

from flypper_redis.storage.redis import RedisStorage

redis = Redis(host="localhost", port=6379, db=0)

def test_list():
    # Cleaning up redis
    for key in redis.scan_iter(match="flypper-test:*"):
        redis.delete(key)

    storage = RedisStorage(redis=redis, prefix="flypper-test")
    assert storage.list() == []

    storage.upsert({
        "name": "fr_api.prod.on_demand_feature",
        "enabled": True,
        "enabled_for_actors": {
            "actor_key": "user_id",
            "actor_ids": ["8", "42", "200000"],
        },
        "enabled_for_percentage_of_actors": None,
        "deleted": False,
    })
    assert len(storage.list()) == 1
    assert storage.list(version__gt=1) == []
