from redis import Redis
from werkzeug.serving import run_simple

from flypper_redis.storage.redis import RedisStorage
from flypper.wsgi.web_ui import FlypperWebUI

redis = Redis(host="localhost", port=6379, db=0)

storage = RedisStorage(redis=redis, prefix="flypper-demo")
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
storage.upsert({
    "name": "fr_api.prod.rolling_out_feature",
    "enabled": True,
    "enabled_for_actors": None,
    "enabled_for_percentage_of_actors": {
        "actor_key": "user_id",
        "percentage": 10.00,
    },
    "deleted": False,
})
storage.upsert({
    "name": "fr_api.prod.fully_rolled_out_feature",
    "enabled": True,
    "enabled_for_actors": None,
    "enabled_for_percentage_of_actors": None,
    "deleted": False,
})
storage.upsert({
    "name": "fr_api.prod.failover_for_payments",
    "enabled": False,
    "enabled_for_actors": None,
    "enabled_for_percentage_of_actors": None,
    "deleted": False,
})

app = FlypperWebUI(storage=storage)
run_simple("127.0.0.1", 5000, app, use_debugger=True, use_reloader=True)
