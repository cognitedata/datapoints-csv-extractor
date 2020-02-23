@Library('jenkins-helpers') _

def label = "datapoints-csv-extractor-${UUID.randomUUID().toString()}"

podTemplate(
    label: label,
    annotations: [
            podAnnotation(key: "jenkins/build-url", value: env.BUILD_URL ?: ""),
            podAnnotation(key: "jenkins/github-pr-url", value: env.CHANGE_URL ?: ""),
    ],
    containers: [
        containerTemplate(name: 'python',
            image: 'python:3.7-slim',
            command: '/bin/cat -',
            resourceRequestCpu: '1000m',
            resourceRequestMemory: '800Mi',
            resourceLimitCpu: '1000m',
            resourceLimitMemory: '800Mi',
            ttyEnabled: true,
            envVars: [
                envVar(key: 'JENKINS_URL', value: env.JENKINS_URL),
                envVar(key: 'BRANCH_NAME', value: env.BRANCH_NAME),
                envVar(key: 'BUILD_NUMBER', value: env.BUILD_NUMBER),
                envVar(key: 'BUILD_URL', value: env.BUILD_URL),
                envVar(key: 'CHANGE_ID', value: env.CHANGE_ID),
            ]),
        containerTemplate(name: 'docker',
            command: '/bin/cat -',
            image: 'docker:18.06.1-ce',
            resourceLimitCpu: '1000m',
            resourceLimitMemory: '1000Mi',
            ttyEnabled: true),
        containerTemplate(name: 'gitleaks',
            command: '/bin/cat -',
            image: 'eu.gcr.io/cognitedata/gitleaks:3.0.2',
            resourceRequestCpu: '300m',
            resourceRequestMemory: '500Mi',
            resourceLimitCpu: '1',
            resourceLimitMemory: '1Gi',
            ttyEnabled: true)
    ],
    volumes: [
        secretVolume(secretName: 'jenkins-docker-builder', mountPath: '/jenkins-docker-builder'),
        hostPathVolume(hostPath: '/var/run/docker.sock', mountPath: '/var/run/docker.sock'),
    ]) {
    node(label) {
        def gitCommit
        container('jnlp') {
            stage('Checkout') {
                scmVars = checkout(scm)
                gitCommit = scmVars.GIT_COMMIT
            }
        }

        container('gitleaks') {
            stage('Gitleaks scan for secrets') {
                sh('gitleaks --repo-path=`pwd` --verbose --redact')
            }
        }

        container('python') {
            stage('Install pipenv') {
                sh("pip3 install pipenv")
            }
            stage('Install dependencies') {
                sh("pipenv sync --dev")
            }
            stage('Check code style') {
                sh("pipenv run black -l 120 --check .")
                //sh("pipenv run isort -w 120 -m 3 -tc -rc --check-only .")
            }
            stage('Run tests and produce coverage report') {
                sh("pipenv run pytest -v --junitxml=test-report.xml")
                junit(allowEmptyResults: true, testResults: '**/test-report.xml')
                summarizeTestResults()
            }
        }

        container('docker') {
            def buildImage = "datapoints-csv-extractor:${gitCommit}"
            def currentVersion = sh(returnStdout: true, script: 'sed -n -e "/^__version__/p" csv_extractor/__init__.py | cut -d\\" -f2').trim()

            stage('Build docker image') {
                sh("docker build -t ${buildImage} .")
                sh('#!/bin/sh -e\n' + 'docker login -u _json_key -p "$(cat /jenkins-docker-builder/credentials.json)" https://eu.gcr.io')
            }

            if (env.CHANGE_ID) {
                stage("Publish PR image") {
                    def prImage = "eu.gcr.io/cognitedata/datapoints-csv-extractor-dev:pr-${env.CHANGE_ID}"
                    sh("docker tag ${buildImage} ${prImage}")
                    sh("docker push ${prImage}")
                    pullRequest.comment("[pr-bot]\nRun this build with `docker run --rm -it ${prImage}`")
                }
            } else if (env.BRANCH_NAME == 'master') {
                stage('Publish to GCR') {
                    def prodImage = "eu.gcr.io/cognite-registry/datapoints-csv-extractor"
                    sh("docker tag ${buildImage} ${prodImage}:${currentVersion}")
                    sh("docker tag ${buildImage} ${prodImage}:latest")
                    sh("docker push ${prodImage}")
                }
            }
        }
    }
}
