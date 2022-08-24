import json
import os
import pprint
import signal
import time
import yaml
import boto3

from pathlib import Path


def setup_aws_resource_service(service_name, access_key_id, secret_access_key, region_name=None):
    return boto3.resource(service_name, aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key,
                          region_name=region_name)


def setup_aws_client_service(service_name, access_key_id, secret_access_key, region_name=None):
    return boto3.client(service_name, aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key,
                        region_name=region_name)


class ResourceReader:

    def __init__(self):
        self.work_dir = Path(__file__).parent
        self.REGION_NAME = 'us-east-1'
        self.EC2 = 'ec2'
        self.S3 = 's3'
        self.IAM = 'iam'
        self.POLICY_ARN = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        self.REDSHIFT = 'redshift'
        self.DESCRIPTION = "Allow Redshift clusters to call AWS services on your behalf."
        self.file_path = None
        self.name = 'dwh.yml'
        self.config = None
        self.redshift_client = None
        self.iam_client = None
        self.iam_role = None
        self.iam_role_arn = None
        self.s3_resource = None
        self.ec2_resource = None

    def load_config(self):
        # loads default configuration from yaml file
        self.file_path = self.resource_resolver(self.name)
        with open(self.file_path, 'r') as f:
            try:
                self.config = yaml.load(f, Loader=yaml.SafeLoader)
            except yaml.YAMLError as exc:
                print(exc)
        return self.config

    def setup_aws_services(self, access_key_id, secret_access_key, region_name=None):
        """
        Initiate services of AWS - IAM, S3, EC2, and Redshift
        :param access_key_id:
        :param secret_access_key:
        :param region_name: region name of AWS; default is my test region ''
        :return: list of service instance
        """
        if not region_name:
            region_name = self.REGION_NAME

        self.iam_client = setup_aws_client_service(self.IAM, access_key_id, secret_access_key, region_name)
        self.redshift_client = setup_aws_client_service(self.REDSHIFT, access_key_id, secret_access_key, region_name)
        self.ec2_resource = setup_aws_resource_service(self.EC2, access_key_id, secret_access_key, region_name)
        self.s3_resource = setup_aws_resource_service(self.S3, access_key_id, secret_access_key, region_name)
        print('Establish IAM, S3, EC2, and Redshift services')

    def create_iam_role(self, configs):
        """
        Create AWS IAM role - Identity and Access Management (IAM) to access S3 AWS Service
            > choose Redshift service (Use case Allow an AWS service like EC2, Lambda, or others to perform actions in this account.)
            > choose Redshift - Customizable
            > Attach permissions policies (search for and select the AmazonS3ReadOnlyAccess policy)
            > enter @RoleName > choose Create Role
            > attach this role to a new/existing cluster
        :param configs: parameters of configuration
        :return: Role of AWS IAM
        """
        try:
            self.iam_role = self.iam_client.create_role(
                Path='/',
                RoleName=configs['IAM_ROLE']['NAME'],
                Description=self.DESCRIPTION,
                AssumeRolePolicyDocument=json.dumps({
                    'Version': '2012-10-17',
                    'Statement': [{
                        'Principal': {'Service': 'redshift.amazonaws.com'},
                        'Resource': '*',
                        'Effect': 'Allow',
                        'Action': 'sts:AssumeRole',
                    }]
                })
            )
            print('IAM role created: ', self.iam_role)
            return self.iam_role
        except Exception as e:
            print(e)

    def apply_policy(self, configs):
        """
        Attach this role to the cluster
        Apply policy for Identity and Access Management (IAM)
        Study case: AmazonS3ReadOnlyAccess
        :param configs:
        :return: None
        """
        response = self.iam_client.attach_role_policy(
            RoleName=configs['IAM_ROLE']['NAME'],
            PolicyArn=self.POLICY_ARN
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print('Attach role policy successful')
        else:
            print('Attach role policy failure. Error: ', response)
        return response

    def get_iam_role_arn(self, configs):
        """
        Get IAM Role ARN
        :param configs: parameter of configuration
        :return: the value of AIM Role ARN
        """
        print(configs['IAM_ROLE']['NAME'])
        self.iam_role_arn = self.iam_client.get_role(RoleName=configs['IAM_ROLE']['NAME'])['Role']['Arn']
        print('IAM role ARN: [{}]'.format(self.iam_role_arn))
        return self.iam_role_arn

    def create_redshift_cluster(self, configs):
        """
        Create Redshift cluster
        :param configs: definition parameters of cluster
        :return:
        """
        try:
            cluster_type = configs['CLUSTER']['TYPE']
            node_type = configs['CLUSTER']['NODE_TYPE']
            node_number = int(configs['CLUSTER']['NUMBER_OF_NODE'])
            identifier = configs['CLUSTER']['CLUSTER_IDENTIFIER']
            db_name = configs['CLUSTER']['DB_NAME']
            db_username = configs['CLUSTER']['DB_USER']
            db_password = configs['CLUSTER']['DB_PASSWORD']
            arn = self.get_iam_role_arn(configs)
            self.init_cluster(cluster_type, node_type, node_number, identifier, db_name, db_username,
                              db_password, arn)
        except Exception as exc:
            print(exc)

    def init_cluster(self, cluster_type, node_type, node_number,
                     cluster_identifier, db_name, db_username,
                     db_password, iam_role_arn):
        """
        Create AWS Redshift cluster
        :param cluster_type: cluster type
        :param node_type: node type
        :param node_number: number of node
        :param cluster_identifier: identifier of cluster
        :param db_name: name of database
        :param db_username: username
        :param db_password: password
        :param iam_role_arn: IAM Role ARN
        :return: None
        """
        try:
            response = self.redshift_client.create_cluster(ClusterType=cluster_type, NodeType=node_type,
                                                           NumberOfNodes=node_number,
                                                           ClusterIdentifier=cluster_identifier,
                                                           DBName=db_name, MasterUsername=db_username,
                                                           MasterUserPassword=db_password, IamRoles=[iam_role_arn])
            print('Redshift cluster created: ', response)
            return response
        except Exception as exc:
            print(exc)

    def tear_down_redshift_cluster(self, configs):
        identifier = configs['CLUSTER']['CLUSTER_IDENTIFIER']
        specifications = self.get_cluster_description(CLUSTER_IDENTIFIER)
        role_name = specifications['IamRoles'][0]['IamRoleArn']

        self.redshift_client.delete_cluster(ClusterIdentifier=identifier, SkipFinalClusterSnapshot=True)
        self.iam_client.detach_role_policy(RoleName=role_name, PolicyArn=self.get_iam_role_arn(configs))
        self.iam_client.delete_role(RoleName=role_name)

    def break_handler(self, signum, frame):
        raise Exception('Redshift creation have been taking too long. Double check it via AWS portal')

    def get_cluster_description(self, identifier):
        """
        Get descriptive cluster
        :return: specification value of cluster description
        """
        return self.redshift_client.describe_clusters(ClusterIdentifier=identifier)['Clusters'][0]

    def configure_incoming_tcp_traffic(self, configs):
        """
        Configure firewall rules for your Redshift cluster to control inbound and outbound traffics.
        To configure traffic rules of VPC (Virtual Private Cloud) following IP, protocol, port ranges.
        By default accept all incoming traffics.
        :param configs: parameter of configuration
        :return: None
        """
        try:
            cluster_specs = self.get_cluster_description(configs['CLUSTER']['CLUSTER_IDENTIFIER'])
            vpc = self.ec2_resource.Vpc(id=cluster_specs['VpcId'])
            port = cluster_specs['Endpoint']['Port']
            security_group = list(vpc.security_groups.all())[0]
            security_group.authorize_ingress(GroupName=security_group.group_name, CidrIp='0.0.0.0/0', IpProtocol='TCP',
                                             FromPort=port, ToPort=port)
        except Exception as e:
            print(e)

    def config_reader(self, name):
        """
        Reads configuration parameter from YAML file
        :param name:
        :return: config instance
        """
        self.name = name
        configs = self.load_config()
        print('Configs loaded!')
        return configs

    def resource_resolver(self, name):
        # change the current working directory
        # os.chdir(os.path)
        # print('work_dir: ', self.work_dir)
        file_path = (self.work_dir / 'resource' / name).resolve()
        return file_path

    def wait_until_cluster_status(self, identifier, status, interval, counter):
        """
        Check cluster state until @status
        :param identifier: identity of Cluster
        :param status: status of Redshift Cluster
        :param interval: retry interval
        :param counter: retrieval counter till timeout
        :return: None
        """
        # Available on UNIX systems only
        # signal.signal(signal.SIGALRM, self.break_handler)
        # signal.alarm(timeout)
        state = None
        while counter:
            state = self.get_cluster_description(identifier)['ClusterStatus']
            print('[{}] fetching retry...'.format(counter))
            if state == status:
                print('Cluster state: ', state)
                return True
            time.sleep(interval)
            counter -= 1
        print('Timeout. Stopped fetching cluster status. Current state: ', state)

    def check_out_s3_resource(self, configs):
        pp = pprint.PrettyPrinter(indent=2)
        bucket = self.s3_resource.Bucket(configs['S3']['BUCKET_NAME'])
        # for obj_sum in my_bucket.objects.all():
        #     obj = self.s3_resource.Object(obj_sum.bucket_name, obj_sum.key)
        #     print(obj.key)

        song_data_files = [f.key for f in bucket.objects.filter(Prefix='song_data')]
        for _ in song_data_files[:5]:
            pp.pprint(_)
        print('SONG DATA COUNT: ', len(song_data_files))

        log_data_files = [f.key for f in bucket.objects.filter(Prefix='log_data')]
        for _ in log_data_files[:5]:
            pp.pprint(_)
        print('LOG DATA COUNT: ', len(log_data_files))

        log_json_data_files = [f.key for f in bucket.objects.filter(Prefix='log_json')]
        for _ in log_json_data_files[:5]:
            pp.pprint(_)
            bucket.download_file(_, _)
            with open(_) as f:
                data = json.load(f)
                pp.pprint(data)
        print('LOG JSON DATA COUNT: ', len(log_json_data_files))


if __name__ == "__main__":
    rr = ResourceReader()
    config = rr.config_reader('dwh.yml')

    CLUSTER_TYPE = config['CLUSTER']['TYPE']
    NODE_NUMBER = int(config['CLUSTER']['NUMBER_OF_NODE'])
    NODE_TYPE = config['CLUSTER']['NODE_TYPE']
    CLUSTER_IDENTIFIER = config['CLUSTER']['CLUSTER_IDENTIFIER']

    DB_NAME = config['CLUSTER']['DB_NAME']
    DB_USERNAME = config['CLUSTER']['DB_USER']
    DB_PASSWORD = config['CLUSTER']['DB_PASSWORD']
    PORT = config['CLUSTER']['DB_PORT']

    ACCESS_KEY_ID = config['AWS']['ACCESS_KEY_ID']
    SECRET_ACCESS_KEY = config['AWS']['SECRET_ACCESS_KEY']
    IAM_ROLE_NAME = config['IAM_ROLE']['NAME']

    WAIT = int(config['CLUSTER']['DEFAULT_WAIT'])
    TIMEOUT = int(config['CLUSTER']['TIMEOUT'])
    region = 'us-east-1'
    rr.setup_aws_services(ACCESS_KEY_ID, SECRET_ACCESS_KEY, region)

    # Create IAM Role if its unavailable
    arn_test = rr.get_iam_role_arn(config)
    if 'arn:aws:iam' not in arn_test:
        print(rr.create_iam_role(config))
    rr.apply_policy(config)
    rr.configure_incoming_tcp_traffic(config)

    # res = rr.create_redshift_cluster(config)
    # paused, resuming, available
    # rr.wait_until_cluster_status(CLUSTER_IDENTIFIER, 'available', WAIT, TIMEOUT)
    specs = rr.get_cluster_description(CLUSTER_IDENTIFIER)
    # print(specs)

    rr.check_out_s3_resource(config)
    # Clean up Redshift cluster
    # rr.tear_down_redshift_cluster(config)

    # s3 = boto3.client('s3')

    # Download log_json_path.json from UDACITY S3 to local
    # my_bucket = s3.Bucket('udacity-dend')

    # log_data_files = [f.key for f in my_bucket.objects.filter(Prefix='log_json_path.json')]
    # my_bucket.download_file(log_data_files[0], 'log_data_files.json')
    # my_bucket.download_file("log_json_path.json", 'log_json_path.json')

    # Check content of downloaded file
    # with open('log_data_files.json') as json_file:
    #     data = json.load(json_file)
    # pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(data)

    # Push file from local to AWS S3
    # rr.s3_resource.upload_file('log_json_path.json', config['S3']['BUCKET_NAME'], rr.get_iam_role_arn(config))
    # rr.s3_resource.upload_file("log_json_path.json", 'log_json_path.json')
