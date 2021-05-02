# start with running:

docker exec -it prefix_finder_redis_1 redis-cli

# then to check the docker is up:

docker-compose ps

# then run the main.py

main.py







# todo the docker for now it complain about don't know the redis module on docker run

docker build --tag gabriel-docker .

docker run gabriel-docker

# check to see if exist

docker images