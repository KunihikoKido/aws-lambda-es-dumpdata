# Elasticsearch Dumpdata for AWS Lambda

## About

#### Runtime
Python 2.7

#### Lambda Hander
lambda_function.lambda_handler

#### Input event

Example: Input event:
```json
{
  "source_host": "http://<your_elasticsearch_server:9200>/",
  "source_index": "blog",
  "scroll": "5m",
  "scan_options": {
    "size": 500
  },
  "s3_bucket": "sourcebucket",
  "s3_prefix": "news"
}

```

* ``source_host``: index to read documents from.
* ``source_index``: index to read documents from.
* ``scroll``: (Optional) keep the scroll open for another minute. default to ``5m``
* ``scan_options.size``: (Optional) you will get back a maximum of size * number_of_primary_shards documents in each batch. default to ``500``
* ``s3_bucket``: s3 bucket name.
* ``s3_prefix``: (optional) s3 object prefix.


#### Execution result

Execution result sample:
```json
{
  "acknowledged": true
}
```

## Setup on local machine
```bash
# 1. Clone this repository with lambda function name
git clone https://github.com/KunihikoKido/aws-lambda-es-dumpdata.git es-daumpdata

# 2. Create and Activate a virtualenv
cd es-daumpdata
virtualenv env
source env/bin/activate

# 3. Install Python modules for virtualenv
pip install -r requirements/local.txt

# 4. Install Python modules for lambda function
fab setup
```

## Run lambda function on local machine
```bash
fab invoke
```

#### Run lambda function with custom event
```bash
fab invoke:custom-event.json
```

## Make zip file
```bash
fab makezip
```

## Update function code on AWS Lambda
```bash
fab aws-updatecode
```
## Get function configuration on AWS Lambda
```bash
fab aws-getconfig
```

## Invoke function on AWS Lambda
```bash
fab aws-invoke
```

## Show fabric Available commands
```bash
fab -l
```
