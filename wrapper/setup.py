import sys
import os
import shutil
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-j", "--jvmpath", dest="jvmpath", default = False, help="path that contain libjvm.so")
parser.add_option("-p", "--prefix", dest="prefix", default = False, help="prefix")
parser.add_option("-a", "--hadooppath", dest="hadooppath", default = False, help="hadoop installation path")

(options, args) = parser.parse_args()

try:
    if sys.argv[1] == 'build':
        print 'Building wrapper script'
        wrapper = open('scribed','r')
        content = wrapper.read()
        content = content.replace('__JVMPATH__',options.jvmpath)
        content = content.replace('__HADOOP_HOME__',options.hadooppath)
        content = content.replace('__prefix__',options.prefix)
        wrapper.close()
        wrapper = open('scribed_out','w')
        wrapper.write(content)
        wrapper.close()
    elif sys.argv[1] == 'install':
        print 'Installing wrapper script'
        shutil.copyfile('scribed_out', '/etc/init.d/scribed')
        os.chmod('/etc/init.d/scribed',755)
    else:
        raise "Wrong command"
except Exception,e :
    print 'Nothing done'
    print e
