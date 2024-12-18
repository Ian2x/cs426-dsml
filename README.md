# CS426 - DSML

## Instructions

To build all the services and export the images, we used Docker Compose:
`sudo docker-compose build`

To launch the Docker Compose stack:
`ALGO=test TEST=0 FAIL=DURING sudo -E docker-compose up`

The `ALGO` environment variable can be set to either `allreduce` or `allreducering`.

The `TEST` environment variable can be set to either `1`, `2`, `3`, or `4`.
These run different test setups.

The `FAIL` environment variable can be set to either `before`, `during`, or `none`.
This controls whether one GPU is randomly selected to fail before group start, during the group execution, or no failures.

## Video Demo (Google Drive)

## Group Work
Ian:

setup file structure, made Dockerfiles, 

Josh:

Both:

discussed/worked on implementation details, the RPCs, etc