#!/bin/bash

DIR=$PWD
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LOCAL_REPO=$PROJECT_DIR/repo
SOURCES=$PROJECT_DIR/depsrc/
GIT_REPOS=( "https://github.com/WolframG/Rhino-Prov-Mod.git" )
ARTIFACTIDS=( "rhino" )
VERSIONS=( "1.7R4-mod-SNAPSHOT" )
GROUPIDS=( "org.mozilla" )
BUILD_CMDS=( "ant jar" )
JAR_FILE=( "build/rhino1_7R5pre/js.jar" )
REVISIONS=( "master" )

echo $PROJECT_DIR
echo $LOCAL_REPO

mkdir -p $SOURCES
mkdir -p $LOCAL_REPO

for (( i=0; i<${#GIT_REPOS[@]}; i++ ));
do
    if [[ $(git clone ${GIT_REPOS[$i]} $SOURCES/${ARTIFACTIDS[$i]}) !=  "Already up-to-date." ]] || [ ! $(mvn -q dependency:get -Dartifact=${GROUPIDS[$i]}:${ARTIFACTIDS[$i]}:${VERSIONS[$i]} -o -DrepoUrl=file://$LOCAL_REPO > /dev/null 2>&1) ] ; then
        cd $SOURCES/${ARTIFACTIDS[$i]}
        git checkout ${REVISIONS[$i]} .
        eval ${BUILD_CMDS[$i]}
        #mvn deploy:deploy-file -Durl=file://$LOCAL_REPO -Dfile=${JAR_FILE[$i]} -DgroupId=${GROUPIDS[$i]} -DartifactId=${ARTIFACTIDS[$i]} -Dpackaging=jar -Dversion=${VERSIONS[$i]}
        mvn clean install:install-file -Dfile=${JAR_FILE[$i]} -DgroupId=${GROUPIDS[$i]} -DartifactId=${ARTIFACTIDS[$i]} -Dpackaging=jar -Dversion=${VERSIONS[$i]}
        cd $DIR
    fi
done

#rm -Rf $SOURCES
