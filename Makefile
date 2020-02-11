IMAGE_NAME=dw-bra

.PHONY: help
help:
	@echo "Usage: make [target]"
	@echo
	@echo "Targets:"
	@echo "  test\t\tLookup for docker and docker-compose binaries"
	@echo "  help\t\tPrint this help"
	@echo "  setup\t\tCreate required directories and build docker images"
	@echo "  run\t\tRun ETL process (Production)"
	@echo "  run-dev\t\tRun ETL process (Development)"
	@echo "  runi\t\tRun interactive shell"

.PHONY: test
test:
	which docker
	which docker-compose

setup: Dockerfile
	#docker-compose pull
	docker image build -t $(IMAGE_NAME) .

.PHONY: run
run:
	docker run -it --rm -v $(PWD):/usr/src/app -p 8050:8050  $(IMAGE_NAME) ./etl.py -v -a

.PHONY: run-dev
run-dev:
	docker run -it --rm -v $(PWD):/usr/src/app -p 8050:8050  $(IMAGE_NAME) ./etl.py -v -a -c config-dev.ini

.PHONY: runi
runi:
	docker run -it --rm -v $(PWD):/usr/src/app -p 8050:8050  $(IMAGE_NAME) bash
