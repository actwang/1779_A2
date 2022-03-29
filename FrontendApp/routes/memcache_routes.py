from flask import Blueprint, request

from models.reply import Reply

from rpc_calls import memcache_rpcs

from extensions import hash_router
from db.db_access import get_all_avail_cache_instances_url
memcache_blueprint = Blueprint('memcache_route', __name__, url_prefix='/api/memcache')


# @memcache_blueprint.route('/stat', methods=['GET'])
# def get_memcache_stat():
#     stats = db_get_cache_stat()
#     if stats is not None or len(stats) > 0:
#         return Reply(success=True, stat=stats).to_json()
#     return Reply(success=False).to_json()


# @memcache_blueprint.route('/set', methods=['POST'])
# def post_memcache_config():
#     capacity = request.form.get('capacity')
#     rep_policy = request.form.get('rep_policy')
#     if db_post_cache_config(capacity, rep_policy):
#         res = memcache_rpcs.call_refresh_configuration()
#         return Reply(success=True).to_json()
#     return Reply(success=False).to_json()


@memcache_blueprint.route('/resize', methods=['POST'])
def post_resize():
    fetch_result = get_all_avail_cache_instances_url()
    caches_url = []
    for res in fetch_result:
        caches_url.append(res[0])
    hash_router.set_caches(caches_url)

    return Reply(success=True).to_json()
