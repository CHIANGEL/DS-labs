import random

def lottery(group_infos, target_group_id):
    # Load balance for READ operation on active servers in target group
    weight_data = {}
    for ServerId, ServerInfo in enumerate(group_infos[int(target_group_id)]):
        weight_data[ServerId] = ServerInfo["weight"]
    total_weight = sum(weight_data.values())
    tmp = random.uniform(0, total_weight)
    curr_sum = 0
    target_server_id = None
    for key in weight_data.keys():
        curr_sum += weight_data[key]
        if tmp <= curr_sum:
            target_server_id = key
            break
    if target_server_id == None:
        target_server_id = 0
    return target_server_id