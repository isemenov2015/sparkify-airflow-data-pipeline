/opt/airflow/start-services.sh
/opt/airflow/start.sh

## Done: update the following bucket name to match the name of your S3 bucket and un-comment it:
#
airflow variables set s3_bucket udacity-aws-sparkify-project
#
## Done: un-comment the below line:
#
airflow variables set s3_prefix data-pipelines

airflow connections add aws_credentials --conn-uri 'aws://AKIA44IGHE6777DAYVHX:RZAV3xi830%2BfqPBfnrPcIMBep%2BVECl5A6JiXVm1d@'

airflow connections add redshift --conn-uri 'redshift://awsuser:R3dsh1ft@default.885313185727.us-east-1.redshift-serverless.amazonaws.com:5439/dev'

airflow users create --email a@email.com --firstname Ilya --lastname Semenov --password admin --role Admin --username admin
