#! /bin/sh

# Argument should be indexer jar to run.
# Wait until we get a 200 from Fedora or fail some number of times.
# Then start indexer.


function wait_until_fedora_up {
    CMD="curl -I -u ${PI_FEDORA_USER}:${PI_FEDORA_PASS} --write-out %{http_code} --silent -o /dev/stderr ${PI_FEDORA_INTERNAL_BASE}"
    echo "Waiting for response from Fedora via ${CMD}"

    RESULT=0
    max=20
    i=1
    
    until [ ${RESULT} -eq 200 ]
    do
        sleep 5
        
        RESULT=$(${CMD})

        if [ $i -eq $max ]
        then
           echo "Reached max attempts"
           exit 1
        fi

        i=$((i+1))
        echo "Trying again, result was ${RESULT}"
    done
    
    echo "Fedora is up."
}

function wait_until_es_up {
    CMD="curl -I --write-out %{http_code} --silent -o /dev/stderr ${PI_ES_BASE}"
    echo "Waiting for response from Elasticsearch via ${CMD}"

    RESULT=0
    max=20
    i=1
    
    until [ ${RESULT} -eq 200 ]
    do
        sleep 5
        
        RESULT=$(${CMD})

        if [ $i -eq $max ]
        then
           echo "Reached max attempts"
           exit 1
        fi

        i=$((i+1))
        echo "Trying again, result was ${RESULT}"
    done

    echo "Elasticsearch is up"
}

wait_until_fedora_up
wait_until_es_up

exec java -Dorg.slf4j.simpleLogger.defaultLogLevel=${PI_LOG_LEVEL} -jar "$1"
