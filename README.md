# ITS_LIVE Monitoring

The ITS_LIVE monitoring stack provides the AWS architecture to support low-latency production of netCDF glacier velocity products produced from Optical (Landsat 8/9, Sentinel-2) and SAR (Sentinel-1) image pairs.

## Architecture overview

ITS_LIVE Monitoring uses a pub-sub model for the optical missions. These Open Data on AWS datasets include SNS Topics to which messages are published for each new scene added to the dataset:
* Landsat: <https://registry.opendata.aws/usgs-landsat/>
* Sentinel-2: <https://registry.opendata.aws/sentinel-2/>

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
* `make test` runs the PyTest test suite.

Review the `Makefile` for a complete list of commands.

### Environment variables

Many parts of this stack are controlled by environment variables. Refer to the `deploy-*.yml` GitHub Actions [workflows](.github/workflows) to see which are set upon deployment. Below is a non-exhaustive list of some environment variables that you may want to set.
* `HYP3_API`: The HyP3 deployment to which jobs will be submitted, e.g. https://hyp3-its-live.asf.alaska.edu.
* `EARTHDATA_USERNAME`: Earthdata Login username for the account which will submit jobs to HyP3. In the production stack, this should the ITS_LIVE operational user; in the test stack, this should be the team testing user.
* `EARTHDATA_PASSWORD`: Earthdata Login password for the account which will submit jobs to HyP3.

### Running the Lambda functions locally

The Lambda functions can be run locally from the command line, or by calling the appropriate function in the Python console.

> [!NOTE]
> To call the functions in the python console, you'll need to add all the `src` directories to your `PYTHONPATH`. With PyCharm, you can accomplish this by marking all such directories as "Sources Root" and enabling the "Add source roots to PYTHONPATH" Python Console setting.

#### Landsat

To show the help text for the [`landsat`](landsat/src/main.py) Lambda function, which is used to submit new Landsat 8/9 scenes for processing:
```shell
python landsat/src/main.py -h
```

For example, processing a valid scene:
```shell
python landsat/src/main.py LC08_L1TP_138041_20240128_20240207_02_T1
```

### Integration tests

The Landsat monitoring Lambda can be tested by manually publishing a message to the test SNS topic which was manually deployed with [`test-sns-cf.yml`](scripts/test-sns-cf.yml).

```shell
aws sns publish \
    --topic-arn ${TOPIC_ARN} \
    --message file://${MESSAGE_FILE}
```

where `TOPIC_ARN` is the ARN of the test topic and `MESSAGE_FILE` is the path to a file containing the contents of the message you want published. Example message contents are provided in these files in the [`tests/integration`](tests/integration) directory:
* [`sns-message-landsat-l9-valid.txt`](tests/integration/sns-message-landsat-l9-valid.txt) - A message containing Landsat 9 scene over ice that *should* be processed.
* [`sns-message-landsat-l9-invalid.txt`](tests/integration/sns-message-landsat-l9-invalid.txt) - A message containingLandsat 9 scene *not* over ice that *should not* be processed.
