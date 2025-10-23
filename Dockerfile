FROM public.ecr.aws/lambda/python:3.12

COPY requirements-its_live_monitoring.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r  requirements-its_live_monitoring.txt

COPY its_live_monitoring/src ${LAMBDA_TASK_ROOT}

COPY requirements-status-messages.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r  requirements-status-messages.txt

COPY status-messages/src ${LAMBDA_TASK_ROOT}

# NOTE: handler set as CMD by  parameter override outside of the Dockerfile
# CMD [ "lambda_function.handler" ]
