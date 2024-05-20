#!/bin/bash
############################################################################################
#### Name: ods_flush.sh                                                                #####
#### Description: Simple bash file to build the docker image for project               #####
#### right time feed.                                                                  #####
####                                                                                   #####
#### Created by: Octavio Sanchez <octavio.sanchez@ibm.com>                             #####
#### Created Date: 2023/11/16                                                          #####
#### Modification:                                                                     #####
####    date       owner       description                                             #####
####***********************************************************************************#####
#### Set the do_run_test variable to FALSE if you like to sikp tests                   #####
#### Add the progress and no-cache flags ind the container_command to troubleshooting  #####
####   --progress=plain                                                                #####
####   --no-cache                                                                      #####
#### Set the do_push flag variable to FALSE to avoid pushing to artifactory            #####
############################################################################################



#check for sbt versions https://github.com/sbt/docker-sbt

imagename=$(cat build.sbt | grep "^name :=" | cut -f 2 -d \" )
version=$(cat build.sbt | grep "^version :=" | cut -f 2 -d \" )
scala=$(cat build.sbt | grep "^scalaVersion :=" | cut -f 2 -d \" | cut -f 1 -d \. )
subversion=$(cat build.sbt | grep "^scalaVersion :=" | cut -f 2 -d \" | cut -f 2 -d \. )
build_base_image='sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.5_8_1.9.0_2.12.18'
run_base_image='docker-na.artifactory.swg-devops.com/txo-dswim-esb-docker-local/spark:2.13_4.0.0_dswdia_SNAPSHOT'
do_run_test='FALSE'
do_push='TRUE'

# Decide the sbt command to run
if [[ ${do_run_test} = 'TRUE' ]]; 
  then echo "Running tests and pack" && 
  sbt_command='sbt test clean pack' && 
  podman_extra="
--secret id=env,src=.env
--secret id=config,src=dswdia_config.conf
--secret id=jks,src=ssl_key.jks
";
  else echo "Packing" && 
  sbt_command='sbt clean pack' &&
  podman_extra=""; 
fi

container_command="podman buildx build
--secret id=credentials,src=${HOME}/.sbt/1.0/credentials.sbt$podman_extra
-t docker-na.artifactory.swg-devops.com/txo-dswim-esb-docker-local/${imagename}:${version}
--build-arg jarfile=target/pack/lib/${imagename}_${scala}.${subversion}-${version}.jar
--build-arg sbt_command='${sbt_command}'
--build-arg build_base_image='${build_base_image}'
--build-arg run_base_image='${run_base_image}'
-f Containerfile ."

echo "Building image ..." &&
eval $container_command

#Check if should push image
if [[ ${do_push} = 'TRUE' ]]; 
  then echo "Pushing image" && 
  push_command="podman push docker-na.artifactory.swg-devops.com/txo-dswim-esb-docker-local/${imagename}:${version}" &&
  eval $push_command;
  else echo "The image wasn't pushed";
fi