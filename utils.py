import tarfile


def decompress(path):

	t = tarfile.open(path, 'r')
	tarfile.extractall(path)
	t.close()
