
# to run on the background the server

docker-compose up -d

# check the docker is up:

docker-compose ps

# then run the main.py

C:\Users\gabri\anaconda3\python.exe main.py
main.py

#to kill on the background

docker kill prefix_finder_redis_1







# todo the docker for now it complain about don't know the redis module on docker run

docker build --tag gabriel-docker .

docker run gabriel-docker

# check to see if exist

docker images


#docker exec -it prefix_finder_redis_1 redis-cli

