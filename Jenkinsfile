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
                'env': [    'MAX_HEAP=256M',
                            'START_JOLOKIA=true',
                            'JOLOKIA_OPTS="port=8000,host=*"'
                        ],
                'sleep': 30,
                'healthcheck': 9042
            ],
            'CASSANDRA': [
                'image': 'stratio/cassandra-lucene-index:%%VERSION',
                'volumes':['jts:1.14.0'],
                'env': [    'MAX_HEAP=256M',
                            'START_JOLOKIA=true',
                            'JOLOKIA_OPTS="port=8000,host=*"',
                            'SEEDS=%%CASSANDRA#0'
                        ],
                'sleep': 30,
                'healthcheck': 9042
            ],
            'CASSANDRA': [
                'image': 'stratio/cassandra-lucene-index:%%VERSION',
                'volumes':['jts:1.14.0'],
                'env': [    'MAX_HEAP=256M',
                            'START_JOLOKIA=true',
                            'JOLOKIA_OPTS="port=8000,host=*"',
                            'SEEDS=%%CASSANDRA#0'
                        ],
                'sleep': 30,
                'healthcheck': 9042
            ]
        ],
    ]


    ATPARAMETERS= """
        | -Dit.host=%%CASSANDRA#0
        | -Dit.monitor_service=jolokia
        | -Dit.monitor_services_url=%%CASSANDRA#0:8000
        | -Dit.replication=1
        | -Dit.consistency=QUORUM
        | -DJACOCO_SERVER=%%CASSANDRA#0"""

    DEV = { config ->
    
        doCompile(config)
        doUT(config)
        doUT(conf: config, parameters: '-Duser.timezone=UTC')
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

        parallel(
            AT_REPLICATION_1: {
                doAT(config)
            },
            AT_REPLICATION_3: {
                def replication3Parameters = [
                    'it.replication': '3',
                    'it.monitor_services_url' : '%%CASSANDRA#0:8000,%%CASSANDRA#1:8000,%%CASSANDRA#2:8000'
                ]
                doAT(conf: config, crossbuild: 'repl_3', parameters: doReplaceTokens(ATPARAMETERS, replication3Parameters))
            },
            failFast: config.FAILFAST
        )
    }
}
