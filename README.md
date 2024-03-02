# ITS_LIVE Monitoring

The ITS_LIVE monitoring stack provides the AWS architecture to support low-latency production of netCDF glacier velocity products produced from Optical (Landsat 8/9, Sentinel-2) and SAR (Sentinel-1) image pairs.

## Architecture overview

For the optical missions, ITS_LIVE Monitoring uses a pub-sub model where a SQS Queue is subscribed to an SNS Topic which posts messages for new scenes available in the dataset's corresponding AWS Open Data bucket: 
* Landsat: <https://registry.opendata.aws/usgs-landsat/>
* Sentinel-2: <https://registry.opendata.aws/sentinel-2/>

Using EvenBridge, an AWS Lambda function consume messages from the SQS Queue and:
* determines if the scene in the message is processable
* searches the dataset's catalog for secondary scenes to form processing pairs
* ensures the scenes haven't already been processed
* submits the scene pairs to <https://hyp3-its-live.asf.alaska.edu> for processing

## Development

### Development environment setup

To create a development environment, run:
```shell
conda env update -f environment.yml
conda activate its-live-monitoring
```

A `Makefile` has been provided to run some common development steps:
* `make static` will run our static analysis suite, which includes `ruff` for linting and formatting of Python code, and `cfn-lin` for linting CloudFormation.
* `make test` will run the PyTest test suite.

You should review the `Makefile` for an exhaustive list of commands.

### Environment variables

Many parts of this stack are controlled by environment variables and you should refer to the `deploy-*.yml` GitHub Actions [workflows](.github/workflows) to see which are set upon deployment. Below is a non-exhaustive list of some environment variables that you may want to set.
* `EARTHDATA_USERNAME`: Earthdata login username for the account which will submit jobs to HyP3. In the production stack, this should the ITS_LIVE operational user; in the test stack, this should be the team testing user. 
* `EARTHDATA_PASSWORD`: Earthdata login password for the account which will submit jobs to HyP3.

### Running the Lambda functions locally

The Lambda functions are designed such that they can be run locally from the command line, or by calling the appropriate function in the Python console.

> [!NOTE]
> To call the functions in the python console, you'll need to add all the `src` directories to your `PYTHONPATH`. With PyCharm, you can accomplish this by marking all such directories as "Source Root" and enabling the "Add source root to PYTHONPATH" Python Console setting. 

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

The Landsat monitoring Lambda can be tested by manually publishing message to the test SNS topic which was manually deployed with [`test-sns-cf.yml`](scripts/test-sns-cf.yml). 

```shell
aws sns publish \
    --topic-arn ${TOPIC_ARN} \
    --message file://${MESSAGE_FILE}
```

where `TOPIC_ARN` is the ARN of the test topic and `MESSAGE_FILE` is the path to a file containing the contents of the message you want published. Example message contents are provided in these files in the [`tests/integration`](tests/integration) directory:
* [`sns-message-landsat-l9-valid.txt`](tests/integration/sns-message-landsat-l9-valid.txt) - A message containing Landsat 9 scene over ice that *should* be processed.
* [`sns-message-landsat-l9-invalid.txt`](tests/integration/sns-message-landsat-l9-invalid.txt) - A message containingLandsat 9 scene *not* over ice that *should not* be processed.
