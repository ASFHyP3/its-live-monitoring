# ITS_LIVE Monitoring

The ITS_LIVE monitoring stack provides the AWS architecture to support low-latency production of netCDF glacier velocity products produced from Optical (Landsat 8/9, Sentinel-2) and SAR (Sentinel-1) image pairs.

## Architecture overview

ITS_LIVE Monitoring uses a pub-sub model all missions. Data providers publish new scene messages to SNS Topics for each new scene added to the dataset. The SNS Topics for each mission data are described on these pages:
* Landsat: <https://registry.opendata.aws/usgs-landsat/>
* Sentinel-2: <https://registry.opendata.aws/sentinel-2/>
* Sentinel-1: <https://github.com/ASFHyP3/CMR-notifier>

ITS_LIVE Monitoring subscribes to these messages and collects them in an SQS Queue. An AWS Lambda function consumes messages from the SQS Queue and:
* determines if the scene in the message should be processed
* searches the dataset's catalog for secondary scenes to form processing pairs
* ensures these pairs haven't already been processed
* submits the scene pairs to HyP3 for processing

## Development

### Development environment setup

To create a development environment, run:
```shell
conda env update -f environment.yml
conda activate its-live-monitoring
```

A `Makefile` has been provided to run some common development steps:
* `make static` runs the static analysis suite, including `ruff` for linting and formatting of Python code, and `cfn-lin` for linting CloudFormation.
* `make tests` runs the PyTest test suite.

Review the `Makefile` for a complete list of commands.

### Environment variables

Many parts of this stack are controlled by environment variables. Below is a non-exhaustive list of some environment variables that you may want to set.
* `HYP3_API`: The HyP3 deployment to which jobs will be submitted, e.g. https://hyp3-its-live.asf.alaska.edu.
* `EARTHDATA_USERNAME`: Earthdata Login username for the account which will submit jobs to HyP3. In the production stack, this should the ITS_LIVE operational user; in the test stack, this should be the team testing user.
* `EARTHDATA_PASSWORD`: Earthdata Login password for the account which will submit jobs to HyP3.
* `JOBS_TABLE_NAME` The jobs table name for the DynamoDB database associated with the HyP3 deployment jobs are submitted to.

 Refer to [`tests/cfg.env`](tests/cfg.env) for a complete list of environment variables.

### Running the Lambda functions locally

The Lambda functions can be run locally from the command line, or by calling the appropriate function in the Python console.

> [!NOTE]
> To call the functions in the python console, you'll need to add all the `src` directories to your `PYTHONPATH`. With PyCharm, you can accomplish this by marking all such directories as "Sources Root" and enabling the "Add source roots to PYTHONPATH" Python Console setting.

#### its_live_monitoring

To show the help text for the [`its_live_monitoring`](its_live_monitoring/src/main.py) Lambda function, which is used to submit new Landsat 8/9 scenes for processing:
```shell
python its_live_monitoring/src/main.py -h
```

For example, processing a valid scene:
```shell
python its_live_monitoring/src/main.py LC08_L1TP_138041_20240128_20240207_02_T1
```

### Integration tests

The `its_live_monitoring` monitoring Lambda can be tested by manually publishing messages to the test SNS topics which was manually provisioned in the AWS Console.

```shell
aws sns publish \
    --topic-arn ${TOPIC_ARN} \
    --message file://${MESSAGE_FILE}
```

where `TOPIC_ARN` is the ARN of the test topic and `MESSAGE_FILE` is the path to a file containing the contents of the message you want published. Example message contents are provided in these files in the [`tests/integration`](tests/integration) directory, two of which are described here:
* [`landsat-l8-valid.json`](tests/integration/landsat-l8-valid.json) - A message containing a Landsat 9 scene over ice that *should* be processed.
* [`landsat-l9-wrong-tier.json`](tests/integration/landsat-l9-wrong-tier.json) - A message containing a Landsat 9 scene *not* over ice that should be *filtered out* and *not* processed.

To submit **all** the integration test payloads to the default test SNS topics, run:
```shell
make integration
```

>[!IMPORTANT]
> The integration tests will submit jobs to `hyp3-its-live-test`, which will publish products to `s3://its-live-data-test`. Notably `s3://its-live-data-test` has a lifecycle rule which will delete all products after 14 days. So to test deduplication of HyP3 and S3, you'll need to:
> 1. disable `hyp3-its-live-test`'s compute environment or start execution manager
> 2. submit the integration tests and see jobs submitted
> 3. submit the integration tests again to see _all_ jobs deduplicate with the hung jobs from the previous step
> 4. re-enable the compute environment or start execution manager and wait for all jobs to finish
> 5. once all jobs are finished, submit the integration tests again to see jobs deduplicate against the products in `s3://its-live-data-test`
>
> That means, fully testing of its-live-monitoring requires _at least_ 3 rounds of integration testing!



To submit _just_ the Landsat integration test payloads to the default Landsat test SNS topic, run:
```shell
make landsat-integration
```
To submit _just_ the Sentinel-1 integration test payloads to the default Sentinel-1 test SNS topic, run:
```shell
make Sentinel1-integration
```
To submit _just_ the Sentinel-2 integration test payloads to the default Sentinel-2 test SNS topic, run:
```shell
make Sentinel2-integration
```

or, you can submit to an alternative SNS topic like:
```shell
LANDSAT_TOPIC_ARN=foobar make landsat-integration
SENTINEL1_TOPIC_ARN=foobar make sentinel1-integration
SENTINEL2_TOPIC_ARN=foobar make sentinel2-integration
```
