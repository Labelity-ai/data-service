FROM python:3.8

RUN pip install prefect[aws]

CMD prefect agent ecs start \
        --token $RUNNER_TOKEN \
        --cluster $CLUSTER_ARN \
        --launch-type $LAUNCH_TYPE \
        --task-role-arn $TASK_ROLE_ARN \
        --name $AGENT_NAME \
        --label $LABELS
