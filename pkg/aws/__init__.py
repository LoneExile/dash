from pkg.config import cfg


class Aws:
    def __init__(self):
        self.access_key = cfg.Aws.AccessKey
        self.secret_key = cfg.Aws.SecretKey
        self.region = cfg.Aws.Region
        self.bucket = cfg.Aws.Bucket
