pipeline {
  agent {
    node {
      label 'java-8'
    }

  }
  stages {
    stage('检出') {
      steps {
        sh 'ci-init'
        checkout([$class: 'GitSCM', branches: [[name: env.GIT_BUILD_REF]], 
                                  userRemoteConfigs: [[url: env.GIT_REPO_URL]]])
      }
    }
  }
}
