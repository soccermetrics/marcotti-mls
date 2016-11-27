.PHONY test install

install:
    pip install -r requirements.txt; python setup.py install

test:
    py.test
