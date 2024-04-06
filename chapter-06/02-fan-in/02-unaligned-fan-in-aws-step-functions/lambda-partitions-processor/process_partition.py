def lambda_handler(event, context):
    print(f'event={event}')
    if event['PartitionNumber'] == 1:
        return False
    else:
        return True
