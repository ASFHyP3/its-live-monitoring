## Development

### Integration Tests

The Landsat monitoring Lambda can be tested by manually publishing message to the test SNS topic which was manually deployed with [`test-sns-cf.yml`](scripts/test-sns-cf.yml). 

```shell
aws sns publish \
    --topic-arn ${TOPIC_ARN} \
    --message file://${MESSAGE_FILE}
```

where `TOPIC_ARN` is the ARN of the test topic and `MESSAGE_FILE` is the path to a file containing the contents of the message you want published. Example message contents are provided in these files in the [`tests/integration`](tests/integration) directory:
* [`sns-message-landsat-l9-valid.txt`](tests/integration/sns-message-landsat-l9-valid.txt) - A message containing Landsat 9 scene over ice that *should* be processed.
* [`sns-message-landsat-l9-invalid.txt`](tests/integration/sns-message-landsat-l9-invalid.txt) - A message containingLandsat 9 scene *not* over ice that *should not* be processed.
