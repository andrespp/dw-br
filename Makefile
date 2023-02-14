IMAGE_NAME=dw-bra

.PHONY: help
help:
	@echo "Usage: make [target]"
	@echo
	@echo "Targets:"
	@echo "  test\t\tLookup for docker and docker-compose binaries"
	@echo "  help\t\tPrint this help"
	@echo "  setup\t\tCreate required directories and build docker images"
	@echo "  getds\t\tDownload datasets"
	@echo "  extractds\tExtract datasets archives"
	@echo "  run\t\tRun ETL process (Production)"
	@echo "  runi\t\tRun interactive shell"

.PHONY: test
test:
	which docker
	which docker-compose

setup: Dockerfile
	docker image build -t $(IMAGE_NAME) .

.PHONY: getds
getds:
	docker run --rm -v $(PWD):/usr/src/app $(IMAGE_NAME) ./get_ds.py

.PHONY: extractds
extractds:
	docker run --rm -v $(PWD):/usr/src/app $(IMAGE_NAME) ./extract_ds.py

.PHONY: run
run:
	docker run --rm -v $(PWD):/usr/src/app $(IMAGE_NAME) ./update-dw

.PHONY: runi
runi:
	docker run -it --rm -v $(PWD):/usr/src/app $(IMAGE_NAME) bash
