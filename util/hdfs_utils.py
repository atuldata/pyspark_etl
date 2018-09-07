from pyspark import SparkContext, SparkConf
import py4j
from EtlLogger import EtlLogger


class hdfs_utils:
    """Convenience methods for interacting with HDFS"""

    def __init__(self, sc, logger):
        self.hadoop_conf = sc._jsc.hadoopConfiguration()
        self.jvm = sc._jvm
        self.logger = logger
        self.hadoop_fs = self.jvm.org.apache.hadoop.fs.FileSystem.get(self.hadoop_conf)
        self.hadoop_trash = self.jvm.org.apache.hadoop.fs.Trash

    def copyFromLocalFile(self, delSrc, src, dst):
        """Copies the file in src on the local filesystem to the HDFS
        directory dst.  If dslSrc is true it will be removed from the
        source filesystem."""

        srcPath = self.jvm.org.apache.hadoop.fs.Path(src)
        dstPath = self.jvm.org.apache.hadoop.fs.Path(dst)
        self.logger.info("Copying %s to %s with delSrc=%r" %
                         (src, dst, delSrc))
        return self.hadoop_fs.copyFromLocalFile(delSrc, srcPath, dstPath)

    def mkdirs(self, dir):
        """Make the given path and all non-existent parents into
        directories.  Has the semantics of Unix 'mkdir -p'."""

        dirPath = self.jvm.org.apache.hadoop.fs.Path(dir)
        self.logger.info("Making directory %s" % dir)
        return self.hadoop_fs.mkdirs(dirPath)

    def rename(self, src, dst):
        """Renames/moves the path src to the path dst."""
        
        srcPath = self.jvm.org.apache.hadoop.fs.Path(src)
        dstPath = self.jvm.org.apache.hadoop.fs.Path(dst)
        self.logger.info("Renaming %s to %s" % (src, dst))
        return self.hadoop_fs.rename(srcPath, dstPath)

    def moveToTrash(self, f):
        """Moves the specified path to HDFS trash"""

        fPath = self.jvm.org.apache.hadoop.fs.Path(f)
        self.logger.info("Moving %s to trash" % f)
        return self.hadoop_trash.moveToAppropriateTrash(self.hadoop_fs,
                                                        fPath,
                                                        self.hadoop_conf)

    def ls(self, glob):
        """Returns the list of files with the given glob pattern
        as FileStatus objects - call getPath/getLen/etc for file
        information.  For example:
        
        >>> [f.getPath().getName() for f in hdfs_utils.ls('/my/path')]
        [u'foo', u'bar']

        >>> [f.getPath().toUri().toString() for f in hdfs_utils.ls('/my/path')]
        [u'hdfs://nameservice1/my/path/foo', u'hdfs://nameservice1/my/path/']

        see JavaDoc for FileStatus:
        https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/fs/FileStatus.html)"""
        
        globPath = self.jvm.org.apache.hadoop.fs.Path(glob)
        self.logger.info("Listing files in %s" % glob)
        return self.hadoop_fs.globStatus(globPath)




if __name__ == "__main__":
    # unit tests

    conf = SparkConf().setAppName('hdfs_utils test')
    sc = SparkContext(conf=conf)
    logger = EtlLogger.get_logger(log_name = 'hdfs_utils', log_level = 'DEBUG')
    
    utils = hdfs_utils(sc, logger)
    
    utils.copyFromLocalFile(False, '/etc/hosts', '/tmp/')
    utils.mkdirs('/tmp/mkdirtest/foo/bar/baz')

    print([f.getPath().getName() for f in utils.ls('/tmp')])
    print([f.getPath().toUri().toString() for f in utils.ls('/tmp/mkdirtest')])

    utils.moveToTrash('/tmp/hosts')
    utils.moveToTrash('/tmp/mkdirtest')


