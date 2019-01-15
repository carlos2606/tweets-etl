import boto3
from utils import decompress


class Load():
	"""The main purpose of this class is to process
	   some Twitter data and load it into a 
	   DynamoDB table"""
	def __init__(self, arg):
		self.arg = arg

decompress("RealUsers.tar.gz")

