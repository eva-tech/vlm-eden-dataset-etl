# Hello reader! This is our repository Makefile
# Here you will find the list of recipes to start
# and test the project. These are meant to be
# used for development.
#
# If you want to read about `make`:
# https://www.gnu.org/software/make/manual/make.html
#
# Why we use it? We can automate tools and it simplified
# long commands and the parameters needed.
# Read more on: https://antonz.org/makefile-automation/
#
# To use you must install build-essentials (Linux)
# or xcode (Mac OSX)
#

# Phony target will execute 'make'
.PHONY: init

# Essential Make CMDs

# This procedure should be enough to get your container running.
init:
	@make down
	@make up
	@make ps

# Will stop the container and remove everything, but the image.
# Read more: https://docs.docker.com/compose/reference/down/
down:
	docker-compose down --volumes --remove-orphans

# Will pull the image defined with CORE_CONTAINER_IMAGE_NAME.
# This will help have faster builds
# Read more: https://docs.docker.com/compose/reference/pull/
pull:
	docker-compose pull

# Will build a container from the docker compose file
# Read more: https://docs.docker.com/compose/reference/build/
build:
	docker-compose build

# Will build a container from the docker compose file
# Read more: https://docs.docker.com/compose/reference/build/
up:
	docker-compose up -d

bash:
	docker exec -it celery_worker_intelligence /bin/bash

postgres:
	docker-compose up -d postgres_intelligence

# Will list the containers
# This is useful for debugging
# Read more: https://docs.docker.com/compose/reference/ps/
ps:
	docker ps
redis:
	docker-compose up redis -d
stop:
	docker-compose stop
logs:
	docker-compose logs -f --tail 100 celery_worker_intelligence flower
