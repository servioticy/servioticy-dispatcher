#!/bin/bash

DIR=$PWD
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LOCAL_REPO=$PROJECT_DIR/repo
SOURCES=$PROJECT_DIR/tmp
GIT_REPOS=( "https://github.com/nopbyte/servioticy-pdp.git" "https://github.com/WolframG/Rhino-Prov-Mod.git" )
ARTIFACTIDS=( "servioticy-pdp" "rhino" )
VERSIONS=( "0.1.0" "1.7R4-mod" )
GROUPIDS=( "de.passau.uni" "org.mozilla" )
BUILD_CMDS=( "gradle jar"  "ant jar" )
JAR_FILE=( "build/libs/PDPComponentServioticy-0.1.0.jar" "build/rhino1_7R5pre/js.jar" )
REVISIONS=( "d379e1d457e0c50e47a9105a89a0b618288c7887" "5fe98c37e9977e6035734b4387b54497c2b35520")

echo $PROJECT_DIR
echo $LOCAL_REPO

mkdir -p $SOURCES
mkdir -p $LOCAL_REPO

for (( i=0; i<${#GIT_REPOS[@]}; i++ ));
do
    if ! mvn dependency:get -Dartifact=${GROUPIDS[$i]}:${ARTIFACTIDS[$i]}:${VERSIONS[$i]} -o -DrepoUrl=file://$LOCAL_REPO ; then
        git clone ${GIT_REPOS[$i]} $SOURCES/${ARTIFACTIDS[$i]}
        cd $SOURCES/${ARTIFACTIDS[$i]}
        git checkout ${REVISIONS[$i]} .
        eval ${BUILD_CMDS[$i]}
        mvn deploy:deploy-file -Durl=file://$LOCAL_REPO -Dfile=${JAR_FILE[$i]} -DgroupId=${GROUPIDS[$i]} -DartifactId=${ARTIFACTIDS[$i]} -Dpackaging=jar -Dversion=${VERSIONS[$i]}
        cd $DIR
    fi
done

rm -R $SOURCES
