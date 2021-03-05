import serverless_sdk
sdk = serverless_sdk.SDK(
    org_id='antoniomdk',
    application_name='data-service',
    app_uid='undefined',
    org_uid='undefined',
    deployment_uid='e2639c7f-b039-4351-92d5-50c6bc247cc8',
    service_name='image-optimization',
    should_log_meta=True,
    should_compress_logs=True,
    disable_aws_spans=False,
    disable_http_spans=False,
    stage_name='dev',
    plugin_version='4.4.3',
    disable_frameworks_instrumentation=False,
    serverless_platform_stage='prod'
)
handler_wrapper_kwargs = {'function_name': 'image-optimization-dev-optimize', 'timeout': 6}
try:
    user_handler = serverless_sdk.get_user_handler('image_processor/handler.main')
    handler = sdk.handler(user_handler, **handler_wrapper_kwargs)
except Exception as error:
    e = error
    def error_handler(event, context):
        raise e
    handler = sdk.handler(error_handler, **handler_wrapper_kwargs)
