def lambda_handler(event, context):
    print(f'event={event}')
    if False in event['ProcessorResults']:
        print('Marking the job as partially valid')
    return True
