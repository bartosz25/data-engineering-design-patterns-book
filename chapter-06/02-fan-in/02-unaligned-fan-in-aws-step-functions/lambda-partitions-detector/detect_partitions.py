def lambda_handler(event, context):
    # let's keep it simple for the demo; IRL it could detect new partitions dynamically
    return list(range(0, 2))
