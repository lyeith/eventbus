"""Application-owned REST API REQUEST authorizer with a fixed local token."""


def handler(event, context):
    authorization = next((value for name, value in event.get('headers', {}).items()
                          if name.lower() == 'authorization'), None)
    if not authorization:
        raise Exception('Unauthorized')
    return {
        'principalId': 'local-example-user',
        'policyDocument': {
            'Version': '2012-10-17',
            'Statement': [{
                'Action': 'execute-api:Invoke',
                'Effect': ('Allow' if authorization == 'Bearer local-authorizer-token'
                           else 'Deny'),
                'Resource': event['methodArn'],
            }],
        },
        'context': {'language': 'python'},
    }
