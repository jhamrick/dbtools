PYCMD=python setup.py
GH_PAGES_SOURCES=src docs VERSION.txt README.md Makefile

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

test:
	nosetests

gh-pages:
	make clean || true
	git checkout gh-pages
	rm -rf _sources _static _modules
	git checkout master $(GH_PAGES_SOURCES)
	git reset HEAD
	pandoc --from markdown --to rst -o README.rst README.md
	make -C docs html
	mv -fv docs/_build/html/* .
	rm -rf $(GH_PAGES_SOURCES) README.rst
	git add -A
	git ci -m "Generated gh-pages for `git log master -1 --pretty=short --abbrev-commit`" && git push origin gh-pages
	git checkout master
