import pandas as pd
import boto3
import json
import time
import configparser
import psycopg2

def create_iam_role():
    iam = boto3.client('iam',aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name='us-west-2'
                      )

    try:
        print('1.1 Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )

    except Exception as e:
        print(e)

    print('1.2 Attaching Policy')
    iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print('1.3 Get the IAM role ARN')
    roleArn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)
    
    return roleArn

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def create_cluster(iAmRole):
    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    
    try:
        print('2.1 Creating a new Redshift cluster')
        response = redshift.create_cluster(
            ClusterType = CLUSTER_TYPE,
            NodeType = NODE_TYPE,
            NumberOfNodes = int(NUM_NODES),
            ClusterIdentifier = CLUSTER_IDENTIFIER,
            DBName = DB_NAME,
            MasterUsername = DB_USER,
            MasterUserPassword = DB_PASSWORD,
            Port = int(DB_PORT),
            IamRoles = [iAmRole]
        )
    except Exception as e:
        print(e)
    
    return redshift


if __name__ == "__main__":
    
    config = configparser.ConfigParser()
    config.read_file(open("dwh.cfg"))
    
    KEY                = config.get("AWS","KEY")
    SECRET             = config.get("AWS","SECRET")
    
    IAM_ROLE_NAME      = config.get("REDSHIFT", "IAM_ROLE_NAME")
    iAmRole = create_iam_role()
    config['IAM_ROLE']['ARN'] = iAmRole
    ROLE_ARN           = config['IAM_ROLE']['ARN']
    
    CLUSTER_TYPE       = config['REDSHIFT']['TYPE']
    NODE_TYPE          = config['REDSHIFT']['NODE_TYPE']
    NUM_NODES          = config['REDSHIFT']['NUM_NODES']
    CLUSTER_IDENTIFIER = config['REDSHIFT']['IDENTIFIER']
    DB_NAME            = config['CLUSTER']['DB_NAME']
    DB_USER            = config['CLUSTER']['DB_USER']
    DB_PASSWORD        = config['CLUSTER']['DB_PASSWORD']
    DB_PORT            = config['CLUSTER']['DB_PORT']
    
    cluster = create_cluster(iAmRole)

    print('2.2 Waiting cluster to be available')
    
    while(cluster.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus'] == "creating"):
        print('Waiting 1 minute more...')
        time.sleep(60)
    
    print('2.3 Redshift cluster available')
    
    config['CLUSTER']['ENDPOINT'] = cluster.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]['Endpoint']['Address']
    CLUSTER_ENDPOINT   = config['CLUSTER']['ENDPOINT']
    
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)
