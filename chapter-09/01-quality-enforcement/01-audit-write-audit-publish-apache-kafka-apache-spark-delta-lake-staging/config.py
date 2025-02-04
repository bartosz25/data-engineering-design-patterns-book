def get_staging_visits_table() -> str:
    return 'visits_staging'


def get_valid_visits_table() -> str:
    return 'visits'


def get_error_table() -> str:
    return 'visits_errors'


def get_visit_event_schema() -> str:
    return '''
            visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING,
            context STRUCT<
                referral STRING, ad_id STRING, 
                user STRUCT<
                    ip STRING, login STRING, connected_since TIMESTAMP
                >,
                technical STRUCT<
                    browser STRING, browser_version STRING, network_type STRING, device_type STRING, device_version STRING
                >
            >
        '''
