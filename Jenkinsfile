@Library('libpipelines@master') _

hose {
    EMAIL = 'cassandra'
    MODULE = 'cassandra-lucene-index'
    DEVTIMEOUT = 90
    RELEASETIMEOUT = 30
    FOSS = true
    REPOSITORY = 'cassandra-lucene-index'    
    LANG = 'java'
    PKGMODULES = ['dist']
    PKGMODULESNAMES = ['stratio-cassandra-lucene-index']
    DEBARCH = 'all'
    RPMARCH = 'noarch'
    EXPOSED_PORTS = [9042, 7199, 8000]

    PARALLELIZEAT = 3

    ATSERVICES =  [
        [
            'CASSANDRA': [
                'image': 'stratio/cassandra-lucene-index:%%VERSION',
                'volumes':['jts:1.14.0'],
                'env': [ 'MAX_HEAP=256M',
                    'START_JOLOKIA=true',
                    'JOLOKIA_OPTS="port=8000,host=*"'],
                'sleep': 30,
                'healthcheck': 9042
            ]
        ],
    ]
    
    ATPARAMETERS = """
        | -Dit.host=%%CASSANDRA
        | -Dit.jmx_port=8000
        | -Dit.monitor_service=jolokia
        | -DJACOCO_SERVER=%%CASSANDRA"""
    
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
