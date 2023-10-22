import configparser


class ConfigReader:
    def __init__(self, config_file_path):
        self.config_file_path = config_file_path
        self.config = configparser.ConfigParser()
        self.config.read(config_file_path)

    def get_aws_iam_config(self):
        aws_iam_config = {}
        if 'aws-iam' in self.config:
            aws_iam_section = self.config['aws-iam']
            aws_iam_config['access_key_id'] = aws_iam_section.get('access_key_id')
            aws_iam_config['access_secret'] = aws_iam_section.get('access_secret')
            aws_iam_config['arn'] = aws_iam_section.get('arn')
        return aws_iam_config

    def get_db_config(self):
        db_config = {}
        if 'db' in self.config:
            db_section = self.config['db']
            db_config['db_name'] = db_section.get('db_name')
            db_config['db_host'] = db_section.get('db_host')
            db_config['db_port'] = db_section.get('db_port')
            db_config['db_login'] = db_section.get('db_login')
            db_config['db_password'] = db_section.get('db_password')
        return db_config

    def get_config_dict(self):
        aws_iam_config = self.get_aws_iam_config()
        db_config = self.get_db_config()
        return {'aws-iam': aws_iam_config, 'db': db_config}