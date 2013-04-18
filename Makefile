PYCMD=python setup.py

all:
	$(PYCMD) bdist

source:
	$(PYCMD) sdist

upload:
	$(PYCMD) sdist upload

install:
	$(PYCMD) install

clean:
	$(PYCMD) clean --all
	rm -rf dist
	rm -f MANIFEST
	rm -f README
	rm -f *.pyc
