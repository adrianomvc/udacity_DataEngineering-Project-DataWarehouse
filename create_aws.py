import pandas as pd
import boto3
import json
import configparser


# Load DWH Params from a file
## CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

## Variables AWS
KEY                 = config.get('AWS','KEY')
SECRET              = config.get('AWS','SECRET')


## Variable DWH
DWH_CLUSTER_TYPE         = config.get('DWH', 'DWH_CLUSTER_TYPE')
DWH_CLUSTER_IDENTIFIER   = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_NUM_NODES            = config.get('DWH', 'DWH_NUM_NODES')
DWH_NODE_TYPE            = config.get('DWH', 'DWH_NODE_TYPE')
DWH_IAM_ROLE_NAME        = config.get('DWH', 'DWH_IAM_ROLE_NAME')

## Variable Cluster
DB_NAME       = config.get('CLUSTER', 'DB_NAME')
DB_USER       = config.get('CLUSTER', 'DB_USER')
DB_PASSWORD   = config.get('CLUSTER', 'DB_PASSWORD')
DB_PORT       = config.get('CLUSTER', 'DB_PORT')   
    

# Create clients for IAM, EC2 and Redshift
## Create IAM
iam = boto3.client('iam',aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   region_name='us-west-2'
                  )

ec2 = boto3.resource('ec2',
                     region_name="us-west-2",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                    )   
## Create redshift
redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                       ) 
    
def create_db():
    
    ## STEP 1: IAM ROLE
    ### Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
    print('1.0: IAM ROLE')
    
    #### TODO 1.1: Create the IAM role
    try:
        print('1.1 Creating a new IAM Role')
        DBRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        ) 

    except Exception as e:
        print(e)
        
    #### TODO 1.2: Attach Policy
    print('1.2 Attaching Policy')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode'] 
    #### TODO 1.3: Get and print the IAM role ARN
    print('1.3 Get the IAM role ARN')
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)
    
    
    ## STEP 2:  Redshift Cluster
    ### Create a RedShift Cluster
    print('2.0 Creating Cluster')
    try:
        response = redshift.create_cluster(        
            #### TODO: add parameters for hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),        

            #### TODO: add parameters for identifiers & credentials
            DBName=DB_NAME,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,        

            #### TODO: add parameter for role (to allow s3 access)
            IamRoles=[roleArn]         

        )
        
        print('X.X End Creating: Wait the cluster status becomes "Available"')
    except Exception as e:
        print(e)
    
        
## 2.1 *Describe* the cluster to see its status        
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def myClusterProps():
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    return myClusterProps


def config_VPC(myClusterProps):    
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
    except Exception as e:
        print(e)
    


def delete_cluster():
    #### CAREFUL!!
    #-- Uncomment & run to delete the created resources
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    
    #### CAREFUL!!
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)
    
    #### CAREFUL!!
    #-- Uncomment & run to delete the created resources
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    #### CAREFUL!!
    
    print('======== Clean Cluster: {}'.format(DWH_CLUSTER_IDENTIFIER)) 