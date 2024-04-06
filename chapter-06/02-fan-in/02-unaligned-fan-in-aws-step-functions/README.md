# Unaligned fan-in - AWS Step Functions

1. Explain the Lambda functions:
* [detect_partitions.py](lambda-partitions-detector%2Fdetect_partitions.py)
  * it implements the partitions detection; although it's hardcoded for the sake of simplicity, you could imagine
    here querying an S3 prefix or a metadata layer to get all new partitions to process
* [process_partition.py](lambda-partitions-processor%2Fprocess_partition.py)
  * this one processes each detected partition individually; again, for the sake of simplicity we're using here
    a Lambda function but you could also use something else, like an Apache Spark job or a SQL query on top of your
    data warehouse layer
* [create_table_from_processed_partitions.py](lambda-table-creator%2Fcreate_table_from_processed_partitions.py)
  * the last function creates the table from the processed results
  * it also detects if any of the processed partitions returned an error; if it's the case, it logs that 
    but typically, it should use this information to flag the table as partial

2. Start Localstack:
```
LAMBDA_DOCKER_NETWORK=host
localstack start -d
```
3. Create the Lambda functions:
* [lambda-partitions-detector](lambda-partitions-detector)
```
rm package_detector.zip
cd lambda-partitions-detector
zip -r ../package_detector.zip .
cd ..
```
* [lambda-partitions-processor](lambda-partitions-processor)
```
rm package_processor.zip
cd lambda-partitions-processor
zip -r ../package_processor.zip .
cd ..
```
* [lambda-table-creator](lambda-table-creator)
```
rm table_creator.zip
cd lambda-table-creator
zip -r ../table_creator.zip .
cd ..
```

3. Create the functions in the AWS local stack runtime:

* [lambda-partitions-detector](lambda-partitions-detector)
```
aws lambda create-function \
    --endpoint-url http://localhost:4566 \
    --function-name partitions_detector \
    --runtime python3.8 \
    --handler detect_partitions.lambda_handler \
    --region us-east-1 \
    --zip-file fileb://./package_detector.zip \
    --role arn:aws:iam::123456789000:role/ignoreme
```
* [lambda-partitions-processor](lambda-partitions-processor)
```
aws lambda create-function \
    --endpoint-url http://localhost:4566 \
    --function-name partitions_processor \
    --runtime python3.8 \
    --handler process_partition.lambda_handler \
    --region us-east-1 \
    --zip-file fileb://package_processor.zip \
    --role arn:aws:iam::123456789000:role/ignoreme
```
* [lambda-table-creator](lambda-table-creator)
```
aws lambda create-function \
    --endpoint-url http://localhost:4566 \
    --function-name table_creator \
    --runtime python3.8 \
    --handler create_table_from_processed_partitions.lambda_handler \
    --region us-east-1 \
    --zip-file fileb://table_creator.zip \
    --role arn:aws:iam::123456789000:role/ignoreme
```

4. Recreate the Step Functions step machine:
```
awslocal stepfunctions delete-state-machine  --state-machine-arn "arn:aws:states:us-east-1:000000000000:stateMachine:fan-in-unaligned"
awslocal stepfunctions create-state-machine --definition file://process_partitions_state_machine.json --name fan-in-unaligned  --role-arn "arn:aws:iam::000000000000:role/stepfunctions-role"
```

The state machine is composed of the following steps:

* The task generating all partitions to process. It reads the _Payload_ property of the Lambda response and  passes it to the next state.
```
"Generate partitions to process": {
  "Type": "Task",
  "Resource": "arn:aws:states:::lambda:invoke",
  "Parameters": {
    "FunctionName": "arn:aws:lambda:us-east-1:000000000000:function:partitions_detector"
  },
  "OutputPath": "$.Payload",
  "Next": "Fan-out hourly partitions"
},
```
    
* The next state is a _Map_ that iterates the returned array (_[0, 1]_) and runs the process partitions  function on each element. 
```
 "Iterator": {
    "StartAt": "Submit Lambda Job",
    "States": {
      "Submit Lambda Job": {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {
          "FunctionName": "arn:aws:lambda:us-east-1:000000000000:function:partitions_processor",
          "Payload":{
            "PartitionNumber.$": "$.PartitionNumber"
          }
        },
        "ResultPath":"$",
        "End":true
      }
    }
  }
```
       
* The last step takes the output from the individual mappers and passes it to the table creation function:
``` 
"Create table":{
  "Type":"Task",
  "InputPath": "$",
  "Resource": "arn:aws:states:::lambda:invoke",
  "Parameters":  {
    "FunctionName": "arn:aws:lambda:us-east-1:000000000000:function:table_creator",
    "Payload":{
      "ProcessorResults.$": "$"
    }
  },
  "Next":"End of Step Function"
}, 
```

5. Run the Step Function:
```
awslocal stepfunctions start-execution  --state-machine-arn "arn:aws:states:us-east-1:000000000000:stateMachine:fan-in-unaligned"
```

6. Check the output:
```
$ docker ps

CONTAINER ID   NAMES
097a155cffae   localstack-main-lambda-table-creator-726eaa2abb250a36b129a6c2ba3d098d
611663c1adc9   localstack-main-lambda-partitions-processor-7968eed84ee1a61fe22629e348ab7978
1dfe67ac7f00   localstack-main-lambda-partitions-processor-2dc94a8fd5d1c3497b73e9b8cc827b64
3279f1a18613   localstack-main-lambda-partitions-detector-c33f026d37f980de3c2f3880d99e6178
```

Use the table-creator function:

```
$ docker logs 097a155cffae
event={'ProcessorResults': [True, False]}
Marking the job as partially valid
```

7. Stop the localstack runtime:
```
localstack stop
```