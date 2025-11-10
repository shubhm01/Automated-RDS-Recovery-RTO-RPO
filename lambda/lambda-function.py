import boto3
import os
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Environment variables
    primary_region = os.environ.get('PRIMARY_REGION', 'us-east-1')
    secondary_region = os.environ.get('SECONDARY_REGION', 'us-west-2')
    parameter_prefix = os.environ.get('PARAM_PREFIX', '/SparkApp/prod')
    database_emails_topic_arn = os.environ['DatabaseEmailsTopic']

    # AWS clients
    ssm_client = boto3.client('ssm', region_name=primary_region)
    rds_client = boto3.client('rds', region_name=primary_region)
    sns_client = boto3.client('sns', region_name=primary_region)
    
    ssm_client_cross_region = boto3.client('ssm', region_name=secondary_region)
    rds_client_cross_region = boto3.client('rds', region_name=secondary_region)

    try:
        # --- Fetch DB parameters ---
        def get_param(param_name, decrypt=False):
            response = ssm_client.get_parameter(Name=f"{parameter_prefix}/{param_name}", WithDecryption=decrypt)
            return response['Parameter']['Value']

        primary_db_identifier = get_param('primary-db-identifier')
        primary_db_endpoint = get_param('primary-db-endpoint')
        primary_db_user = get_param('primary-db-user')
        primary_db_password = get_param('primary-db-password', decrypt=True)
        
        secondary_db_identifier = get_param('secondary-db-identifier')
        secondary_db_endpoint = get_param('secondary-db-endpoint')

        logger.info(f"Primary DB: {primary_db_identifier}, Secondary DB: {secondary_db_identifier}")

        # --- Check primary DB status ---
        primary_db_instance = get_db_instance_response(rds_client, primary_db_identifier)
        primary_db_status = primary_db_instance['DBInstances'][0]['DBInstanceStatus']

        if primary_db_status == 'available':
            logger.info(f"✅ Primary DB {primary_db_identifier} is healthy and available.")
            return {"status": "healthy"}

        logger.warning(f"⚠️ Primary DB {primary_db_identifier} unavailable. Status: {primary_db_status}")
        
        # --- Check replica in secondary region ---
        secondary_db_instance = get_db_instance_response(rds_client_cross_region, secondary_db_identifier)
        secondary_db_status = secondary_db_instance['DBInstances'][0]['DBInstanceStatus']

        if secondary_db_status == 'available':
            logger.info("Replica available — promoting read replica.")
            promote_read_replica(rds_client_cross_region, secondary_db_identifier)
            send_sns_alert(sns_client, database_emails_topic_arn,
                           "RDS Failover Triggered",
                           "Primary DB is down. Promoted the cross-region read replica.")
        else:
            logger.error("Both primary and replica databases are unavailable!")
            send_sns_alert(sns_client, database_emails_topic_arn,
                           "Critical: All RDS Instances Down",
                           "Both Primary and Replica RDS instances are unavailable.")

    except Exception as e:
        logger.exception(f"❌ Error during RDS recovery check: {str(e)}")


def promote_read_replica(rds_client, db_identifier):
    try:
        rds_client.promote_read_replica(DBInstanceIdentifier=db_identifier)
        logger.info(f"✅ Promoting replica {db_identifier} to primary.")
    except Exception as e:
        logger.error(f"Error promoting replica: {str(e)}")


def get_db_instance_response(rds_client, instance_identifier):
    return rds_client.describe_db_instances(DBInstanceIdentifier=instance_identifier)


def send_sns_alert(sns_client, topic_arn, subject, message):
    sns_client.publish(TopicArn=topic_arn, Subject=subject, Message=message)
    logger.info(f"SNS alert sent: {subject}")
