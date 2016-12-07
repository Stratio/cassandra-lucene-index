@Library('libpipelines@master') _

hose {
    EMAIL = 'cassandra'
    MODULE = 'cassandra-lucene-index'
    DEVTIMEOUT = 50
    RELEASETIMEOUT = 30
    FOSS = true
    REPOSITORY = 'cassandra-lucene-index'    
    LANG = 'java'
    PKGMODULES = ['plugin']
    PKGMODULESNAMES = ['stratio-cassandra-lucene-index']
    DEBARCH = 'all'
    RPMARCH = 'noarch'
    EXPOSED_PORTS = [9042, 7199, 8000]

    PARALLELIZE_AT = true

    ATSERVICES =  [
        ['CASSANDRA': [
           'image': 'stratio/cassandra-lucene-index:%%VERSION',
           'volumes':[
                 'jts:1.14.0'],
           'env': [
                 'MAX_HEAP=256M',
                  'START_JOLOKIA=true',
                  'JOLOKIA_OPTS="port=8000,host=$(hostname --ip)"'],
           'sleep': 10]],
        ]
    
    ATPARAMETERS = """
        | -Dit.host=%%CASSANDRA
        | -Dit.monitor_service=jolokia
        | -Dit.monitor_services_url=%%CASSANDRA:8000
        | -DJACOCO_SERVER=%%CASSANDRA
        | -Dit-embedded=false"""
    
    DEV = { config ->
    
        doCompile(config)
        doUT(config)
        doPackage(config)

        parallel(DOC: {
            doDoc(config)
        }, QC: {
            doStaticAnalysis(config)
        }, DEPLOY: {
            doDeploy(config)
        }, DOCKER : {    
            doDocker(config)
        }, failFast: config.FAILFAST)

        doAT(config)
    }
}
