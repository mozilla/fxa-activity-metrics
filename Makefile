SYSTEMPYTHON = `which python2 python | head -n 1`
VIRTUALENV = virtualenv --python=$(SYSTEMPYTHON)
ENV = ./build
PIP_INSTALL = $(ENV)/bin/pip install


.PHONY: all
all: build

.PHONY: build
build: | $(ENV)/COMPLETE
$(ENV)/COMPLETE:
	$(VIRTUALENV) --no-site-packages $(ENV)
	$(PIP_INSTALL) boto postgres
	touch $(ENV)/COMPLETE

.PHONY: import
import: | $(ENV)/COMPLETE
	$(ENV)/bin/python ./import_activity_events.py
	$(ENV)/bin/python ./import_flow_events.py
	$(ENV)/bin/python ./import_email_events.py
	$(ENV)/bin/python ./import_counts.py

.PHONY: summarize
summarize: | $(ENV)/COMPLETE
	$(ENV)/bin/python ./calculate_daily_summary.py
