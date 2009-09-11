DESTDIR ?= /
EXTRA_DIST = setup.py scribe

all: 

all-local:
	$(PYTHON) setup.py build --jvmpath=$(jvm_lib) --hadooppath=$(hadoop_home) --prefix=$(prefix)

install-exec-hook:
	$(PYTHON) setup.py install

clean:	clean-local

clean-local:

check-local: all
