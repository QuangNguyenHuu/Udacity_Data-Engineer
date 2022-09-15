import os

import boto3
import json
import pprint
import time
import yaml

from botocore.exceptions import ClientError
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
        self.s3_client = None
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
    
    def resource_path_builder(self, *args):
        # change the current working directory
        # os.chdir(os.path)
        path = None
        for arg in args:
            if not path:
                path =  (self.work_dir / 'resource').resolve()
            path = (path / arg).resolve()
        return path

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

        # self.iam_client = setup_aws_client_service(self.IAM, access_key_id, secret_access_key, region_name)
        # self.redshift_client = setup_aws_client_service(self.REDSHIFT, access_key_id, secret_access_key, region_name)
        self.s3_client = setup_aws_client_service(self.S3, access_key_id, secret_access_key, region_name)
        self.s3_resource = setup_aws_resource_service(self.S3, access_key_id, secret_access_key, region_name)
        # self.ec2_resource = setup_aws_resource_service(self.EC2, access_key_id, secret_access_key, region_name)
        print('Establish AWS services')

    def check_out_s3_resource(self, configs):
        """
        Browses song data & log data on S3 of AWS
        :param configs:
        :return:
        """
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

    def create_s3_bucket(self, configs):
        """
        Creates S3 Bucket
        :param configs: configuration of S3 bucket
        :return: details of response request
        """
        bucket_name = configs['S3']['BUCKET_NAME']
        location = configs['AWS']['REGION']
        try:
            response = self.s3_client.create_bucket(Bucket=bucket_name, ACL='public-read')
            print('Bucket named [{}] created'.format(bucket_name))
            return response
        except ClientError as exc:
            print('Create S3 bucket failed: ', exc)
            exit()

    def upload_file(self, file_name, bucket_name, object_name=None):
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
        Upload a file to an S3 bucket
        :param file_name: File to upload
        :param bucket_name: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        if object_name is None:
            object_name = os.path.basename(file_name)
        try:
            self.s3_client.upload_file(file_name, bucket_name, object_name)
            print('Upload file [{}] completed'.format(file_name))
        except ClientError as e:
            print(e)
            return False
        return True

    def delete_s3_bucket(self, name):
        """
        Deletes S3 Bucket
        :param name: name of S3 bucket
        :return: details of response request
        """
        response = self.s3_client.list_buckets()
        try:
            # ExpectedBucketOwner, the account ID of the expected bucket owner. If the bucket is owned by a different account,
            # the request fails with the HTTP status code 403 Forbidden (access denied).
            for _ in response['Buckets']:
                print(_['Name'])
                if _['Name'] == name:
                    bucket = self.s3_resource.Bucket(name)
                    bucket.objects.all().delete()
                    print('All resources in bucket [{}] removed completely'.format(name))
                    response = self.s3_client.delete_bucket(Bucket=name)
                    print('Bucket [{}] has deleted'.format(name))
                    return response
            print('Bucket [{}] is NOT found')
        except ClientError as exc:
            print('Delete S3 bucket failed: ', exc)


if __name__ == "__main__":
    rr = ResourceReader()
    config = rr.config_reader('dwh.yml')

    # CLUSTER_TYPE = config['CLUSTER']['TYPE']
    # NODE_NUMBER = int(config['CLUSTER']['NUMBER_OF_NODE'])
    # NODE_TYPE = config['CLUSTER']['NODE_TYPE']
    # CLUSTER_IDENTIFIER = config['CLUSTER']['CLUSTER_IDENTIFIER']

    DB_NAME = config['CLUSTER']['DB_NAME']
    DB_USERNAME = config['CLUSTER']['DB_USER']
    DB_PASSWORD = config['CLUSTER']['DB_PASSWORD']
    PORT = config['CLUSTER']['DB_PORT']

    ACCESS_KEY_ID = config['AWS']['ACCESS_KEY_ID']
    SECRET_ACCESS_KEY = config['AWS']['SECRET_ACCESS_KEY']
    IAM_ROLE_NAME = config['IAM_ROLE']['NAME']
    BUCKET_NAME = config['S3']['BUCKET_NAME']
    REGION = config['AWS']['REGION']
    WAIT = int(config['CLUSTER']['DEFAULT_WAIT'])
    TIMEOUT = int(config['CLUSTER']['TIMEOUT'])
   
    rr.setup_aws_services(ACCESS_KEY_ID, SECRET_ACCESS_KEY, REGION)
    response = rr.s3_client.list_buckets()
    
    # rr.create_s3_bucket(config)
    # rr.delete_s3_bucket('myeyesbucket1.test')

    # Create IAM Role if its unavailable
    # arn_test = rr.get_iam_role_arn(config)
    # if 'arn:aws:iam' not in arn_test:
    #     print(rr.create_iam_role(config))
    # rr.apply_policy(config)
    # rr.configure_incoming_tcp_traffic(config)

    # res = rr.create_redshift_cluster(config)
    # paused, resuming, available
    # rr.wait_until_cluster_status(CLUSTER_IDENTIFIER, 'available', WAIT, TIMEOUT)
    # specs = rr.get_cluster_description(CLUSTER_IDENTIFIER)
    # print(specs)

    # rr.check_out_s3_resource(config)
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
